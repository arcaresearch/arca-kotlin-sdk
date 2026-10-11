package network.arca.sdk

import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.AggregationSource
import network.arca.sdk.models.AggregationSourceType
import network.arca.sdk.models.ConnectionStatus
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.Base64
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

/**
 * Regression for the Gobi WebSocket reconnect loop (alert-reduction item 28).
 *
 * The P&L chart's aggregation watch was created over REST and attached to the
 * socket. The watch registry is per pod and the server destroys a watch with
 * the connection that registered it, so after any reconnect the replayed
 * attach answered "unknown watch". The attach carried no requestId, so the
 * error read as connection-level: the SDK cancelled the socket, reconnected a
 * second later (auth had reset the backoff) and replayed the same attach.
 * Production saw ~6k connections an hour, 87% from Android, most living under
 * a second.
 */
class AggregationWatchReconnectLoopTest {

    private val managers = Collections.synchronizedList(mutableListOf<WebSocketManager>())
    private val servers = Collections.synchronizedList(mutableListOf<MockWebServer>())

    @AfterEach
    fun tearDown() {
        synchronized(managers) { managers.toList() }.forEach { runCatching { it.shutdown() } }
        synchronized(servers) { servers.toList() }.forEach { runCatching { it.shutdown() } }
    }

    @Test
    fun unknownWatchAnswerToAnAttachKeepsTheSocketAndDropsTheWatch() = runBlocking {
        val (ws, factory) = manager()
        ws.connect()
        factory[0].deliver(AUTHENTICATED)

        ws.attachAggregationWatch("wch_gone")
        val attach = factory[0].lastFrame("attach_aggregation_watch")
        val requestId = attach.string("requestId")
        assertTrue(requestId.isNotEmpty(), "an attach must carry a requestId so its error is request-scoped")
        factory[0].deliver("""{"type":"error","message":"unknown watch","requestId":"$requestId"}""")
        delay(50)

        assertFalse(factory[0].canceled, "a request-scoped error must not drop the socket")
        assertEquals(ConnectionStatus.CONNECTED, ws.status)
        assertEquals(1, factory.count)

        // The dropped watch is not replayed onto the next socket.
        assertTrue(ws.rotateConnection())
        factory[1].deliver(AUTHENTICATED)
        delay(50)
        assertFalse(factory[1].actions().contains("attach_aggregation_watch"), factory[1].sent.toString())
    }

    @Test
    fun errorForAnUntrackedRequestKeepsTheSocket() = runBlocking {
        val (ws, factory) = manager()
        ws.connect()
        factory[0].deliver(AUTHENTICATED)

        factory[0].deliver("""{"type":"error","message":"unknown watch","requestId":"timed-out-long-ago"}""")
        delay(50)

        assertFalse(factory[0].canceled)
        assertEquals(ConnectionStatus.CONNECTED, ws.status)
    }

    @Test
    fun attachWaitsForAuth() = runBlocking {
        val (ws, factory) = manager()
        ws.connect()

        // Sent before auth, the server answers "not authenticated" with no
        // requestId: a connection-level error.
        ws.attachAggregationWatch("wch_1")
        assertEquals(listOf("auth"), factory[0].actions())

        factory[0].deliver(AUTHENTICATED)
        assertEquals("wch_1", factory[0].lastFrame("attach_aggregation_watch").string("watchId"))
    }

    @Test
    fun errorWithoutARequestIdAfterAuthenticationKeepsTheSocket() = runBlocking {
        val (ws, factory) = manager()
        ws.connect()
        factory[0].deliver(AUTHENTICATED)

        // A bad id stays in the subscription set; reconnecting would resend it.
        factory[0].deliver(NON_CANONICAL_COIN)
        delay(50)

        assertFalse(factory[0].canceled, "an authenticated connection must survive a rejected subscription")
        assertEquals(ConnectionStatus.CONNECTED, ws.status)
        assertEquals(1, factory.count)
    }

    @Test
    fun errorBeforeAuthenticationStillReconnects() = runBlocking {
        val (ws, factory) = manager()
        ws.connect()

        factory[0].deliver(AUTH_REJECTED)

        assertTrue(factory[0].canceled, "a refused authentication leaves the socket unusable")
        eventually(3_000) { factory.count == 2 }
    }

    @Test
    fun errorWithoutARequestIdOnAnAuthenticatedHandoffDoesNotAbortTheRotation() = runBlocking {
        val (ws, factory) = manager()
        ws.connect()
        factory[0].deliver(AUTHENTICATED)

        assertTrue(ws.rotateConnection())
        factory[1].deliver(AUTHENTICATED)
        factory[1].deliver(NON_CANONICAL_COIN)
        factory[1].deliver(PONG)
        delay(60)

        assertTrue(factory[0].canceled, "the replacement should have taken over")
        assertFalse(factory[1].canceled)
    }

    @Test
    fun rejectedAttachOnTheWarmingSocketDoesNotAbortTheRotation() = runBlocking {
        val (ws, factory) = manager()
        ws.connect()
        factory[0].deliver(AUTHENTICATED)
        ws.attachAggregationWatch("wch_on_other_pod")

        assertTrue(ws.rotateConnection())
        factory[1].deliver(AUTHENTICATED)
        val requestId = factory[1].lastFrame("attach_aggregation_watch").string("requestId")
        factory[1].deliver("""{"type":"error","message":"unknown watch","requestId":"$requestId"}""")
        factory[1].deliver(PONG)
        delay(60)

        assertTrue(factory[0].canceled, "the replacement should have taken over")
        assertFalse(factory[1].canceled)
    }

    @Test
    fun watchAggregationCreatesItsWatchOnTheSocketAndRecreatesItAfterAReconnect() = runBlocking {
        val h = arcaHarness()
        val pending = async { h.arca.watchAggregation(SOURCES) }
        delay(50)
        h.factory[0].deliver(AUTHENTICATED)
        h.factory[0].answerCreate("wch_1")
        val stream = withTimeout(5_000) { pending.await() }

        assertEquals("wch_1", stream.watchId)
        assertEquals(0, h.dispatcher.restCreates.get(), "the watch must not be created over REST")

        // The socket drops; the replacement must get a fresh watch, never a
        // replayed attach for the one that died with the old connection.
        h.factory[0].fail()
        eventually { stream.state.value == WatchStreamState.RECONNECTING }
        eventually(3_000) { h.factory.count == 2 }
        h.factory[1].deliver(AUTHENTICATED)
        h.factory[1].answerCreate("wch_2")

        eventually { stream.state.value == WatchStreamState.CONNECTED }
        assertFalse(h.factory[1].actions().contains("attach_aggregation_watch"), h.factory[1].sent.toString())
        assertFalse(h.factory[1].canceled)

        // Updates for the new watch reach the stream; the dead one's do not.
        h.factory[1].deliver(aggregationUpdated("wch_1", "111"))
        h.factory[1].deliver(aggregationUpdated("wch_2", "2000"))
        eventually { stream.aggregation.value?.totalEquityUsd == "2000" }
        assertEquals(0, h.dispatcher.restCreates.get())
        stream.stop()
    }

    @Test
    fun createInFlightWhenTheSocketDropsIsResentOnTheNextOne() = runBlocking {
        val h = arcaHarness()
        val pending = async { h.arca.watchAggregation(SOURCES) }
        delay(50)
        h.factory[0].deliver(AUTHENTICATED)
        h.factory[0].answerCreate("wch_1")
        val stream = withTimeout(5_000) { pending.await() }

        h.factory[0].fail()
        eventually(3_000) { h.factory.count == 2 }
        h.factory[1].deliver(AUTHENTICATED)
        eventually { h.factory[1].actions().contains("create_aggregation_watch") }
        val inFlight = h.factory[1].lastFrame("create_aggregation_watch").string("requestId")

        // Unanswered when its socket dies: the re-create must not hang on a
        // reply that can no longer arrive.
        h.factory[1].fail()
        eventually(5_000) { h.factory.count == 3 }
        h.factory[2].deliver(AUTHENTICATED)
        eventually { h.factory[2].actions().contains("create_aggregation_watch") }
        assertEquals(inFlight, h.factory[2].lastFrame("create_aggregation_watch").string("requestId"))
        h.factory[2].answerCreate("wch_3")

        eventually { stream.state.value == WatchStreamState.CONNECTED }
        h.factory[2].deliver(aggregationUpdated("wch_3", "3000"))
        eventually { stream.aggregation.value?.totalEquityUsd == "3000" }
        stream.stop()
    }

    @Test
    fun backoffDoesNotResetUntilTheConnectionHasStayedUp() = runBlocking {
        val records = Collections.synchronizedList(mutableListOf<ArcaLogRecord>())
        val (ws, factory) = manager(ArcaLogger(ArcaLogLevel.WARNING) { records.add(it) })
        ws.connect()

        factory[0].deliver(AUTHENTICATED)
        factory[0].fail()
        eventually(3_000) { factory.count == 2 }
        factory[1].deliver(AUTHENTICATED)
        factory[1].fail()

        eventually {
            synchronized(records) { records.count { it.message == "scheduling reconnect" } } == 2
        }
        val delays = synchronized(records) {
            records.filter { it.message == "scheduling reconnect" }.map { it.metadata["delaySeconds"] }
        }
        assertEquals(listOf("1.0", "2.0"), delays, "a socket that fails right after auth must keep backing off")
    }

    @Test
    fun ensureConnectedDuringBackoffOpensNoExtraSocket() = runBlocking {
        val (ws, factory) = manager()
        ws.connect()
        factory[0].deliver(AUTHENTICATED)
        factory[0].fail()

        ws.ensureConnected()
        assertEquals(1, factory.count, "a scheduled reconnect is already connecting")

        delay(1_400)
        assertEquals(2, factory.count, "exactly the scheduled reconnect opens a socket")
        assertFalse(factory[1].canceled)
    }

    @Test
    fun foregroundDuringBackoffConnectsNowAndKeepsBackingOff() = runBlocking {
        val records = Collections.synchronizedList(mutableListOf<ArcaLogRecord>())
        val (ws, factory) = manager(ArcaLogger(ArcaLogLevel.WARNING) { records.add(it) })
        ws.connect()
        factory[0].deliver(AUTHENTICATED)
        ws.handleAppDidEnterBackground()
        factory[0].fail()

        ws.handleAppWillEnterForeground()
        assertEquals(2, factory.count, "returning to the app must not wait out the backoff")

        delay(1_400)
        assertEquals(2, factory.count, "the superseded scheduled reconnect must not open a third socket")
        assertFalse(factory[1].canceled)

        factory[1].deliver(AUTHENTICATED)
        factory[1].fail()
        eventually {
            synchronized(records) { records.count { it.message == "scheduling reconnect" } } == 2
        }
        val delays = synchronized(records) {
            records.filter { it.message == "scheduling reconnect" }.map { it.metadata["delaySeconds"] }
        }
        assertEquals(listOf("1.0", "2.0"), delays, "the foreground connect must not reset the backoff")
    }

    // MARK: - Harness

    private fun manager(log: ArcaLogger = ArcaLogger.disabled): Pair<WebSocketManager, FakeSocketFactory> {
        val factory = FakeSocketFactory()
        val ws = WebSocketManager(
            baseUrl = "http://localhost:3052",
            token = "test",
            realmId = "rlm_test",
            httpClient = OkHttpClient(),
            log = log,
            connectionLifetimeMs = 0,
            socketFactory = factory,
        )
        managers.add(ws)
        return ws to factory
    }

    private class ArcaHarness(val arca: Arca, val factory: FakeSocketFactory, val dispatcher: RestCreateCounter)

    private fun arcaHarness(): ArcaHarness {
        val dispatcher = RestCreateCounter()
        val server = MockWebServer().apply { this.dispatcher = dispatcher; start() }
        servers.add(server)
        val baseUrl = server.url("/").toString().trimEnd('/')
        val http = OkHttpClient()
        val (ws, factory) = manager()
        val arca = Arca(
            realmId = "rlm_test",
            candleCdnBaseUrl = null,
            client = ArcaClient(token = FAKE_JWT, baseUrl = baseUrl, httpClient = http, logger = ArcaLogger.disabled),
            ws = ws,
            tokenManager = TokenManager(null),
            historyCache = HistoryCache(),
            log = ArcaLogger.disabled,
            httpClient = http,
        )
        return ArcaHarness(arca, factory, dispatcher)
    }

    private suspend fun eventually(timeoutMs: Long = 2_000, condition: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (!condition()) {
            if (System.currentTimeMillis() > deadline) throw AssertionError("condition not met within ${timeoutMs}ms")
            delay(10)
        }
    }

    private fun FakeWebSocket.lastFrame(action: String): JsonObject =
        sent.map { arcaJson.parseToJsonElement(it).jsonObject }
            .last { it["action"]?.jsonPrimitive?.content == action }

    private fun JsonObject.string(key: String): String = this[key]?.jsonPrimitive?.content ?: ""

    private suspend fun FakeWebSocket.answerCreate(watchId: String) {
        eventually { actions().contains("create_aggregation_watch") }
        val requestId = lastFrame("create_aggregation_watch").string("requestId")
        deliver(
            """{"type":"aggregation_watch_created","requestId":"$requestId","watchId":"$watchId","aggregation":$AGGREGATION}""",
        )
    }

    private class RestCreateCounter : Dispatcher() {
        val restCreates = AtomicInteger(0)

        override fun dispatch(request: RecordedRequest): MockResponse {
            val path = (request.path ?: "").substringBefore("?")
            if (path.endsWith("/aggregations/watch") && request.method == "POST") restCreates.incrementAndGet()
            return MockResponse().setResponseCode(404)
        }
    }

    private companion object {
        const val AUTHENTICATED = """{"type":"authenticated"}"""
        const val PONG = """{"type":"pong"}"""
        const val AUTH_REJECTED = """{"type":"error","message":"Authentication failed"}"""
        const val NON_CANONICAL_COIN =
            """{"type":"error","message":"coin \"BTC\" is not in canonical format — use e.g. hl:0:BTC, hl:1:SILVER; ignored, kept 0 valid subscription(s)"}"""
        const val AGGREGATION =
            """{"prefix":"/users","totalEquityUsd":"1000","departingUsd":"0","breakdown":[]}"""

        val SOURCES = listOf(AggregationSource(AggregationSourceType.PREFIX, "/users"))

        val FAKE_JWT: String = run {
            val enc = Base64.getUrlEncoder().withoutPadding()
            val header = enc.encodeToString("""{"alg":"HS256","typ":"JWT"}""".toByteArray())
            val payload = enc.encodeToString("""{"realmId":"rlm_test","sub":"usr_test"}""".toByteArray())
            "$header.$payload.fakesig"
        }

        fun aggregationUpdated(watchId: String, equity: String): String =
            """{"type":"aggregation.updated","entityId":"$watchId",""" +
                """"aggregation":{"prefix":"/users","totalEquityUsd":"$equity","departingUsd":"0","breakdown":[]}}"""
    }
}
