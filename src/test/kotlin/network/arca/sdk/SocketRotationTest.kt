package network.arca.sdk

import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import network.arca.sdk.models.AggregationSource
import network.arca.sdk.models.AggregationSourceType
import network.arca.sdk.models.ConnectionStatus
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import okio.ByteString
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.Base64
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

/**
 * Covers gapless socket rotation: a replacement socket is warmed up beside the
 * live one and only takes over once the server has confirmed its subscriptions
 * are registered.
 *
 * The manager's own [WebSocketManager.injectMessage] hook cannot express any of
 * this — it drives a single implicit socket and knows nothing about which one a
 * frame arrived on. These tests go through [FakeSocketFactory] instead, which
 * hands back sockets the test can address individually, so cross-socket
 * ordering (warm, resubscribe, barrier pong, retire) is exact rather than
 * timing-dependent.
 */
class SocketRotationTest {

    private val managers = Collections.synchronizedList(mutableListOf<WebSocketManager>())
    private val servers = Collections.synchronizedList(mutableListOf<MockWebServer>())

    @AfterEach
    fun tearDown() {
        synchronized(managers) { managers.toList() }.forEach { runCatching { it.shutdown() } }
        managers.clear()
        synchronized(servers) { servers.toList() }.forEach { runCatching { it.shutdown() } }
        servers.clear()
    }

    // MARK: - 1. Warming

    @Test
    fun warmsSecondSocketWithoutClosingTheFirstOrChangingStatus() = runBlocking {
        val h = harness()
        val statuses = h.recordStatuses()
        h.connectAndAuth()
        delay(50)
        statuses.clear()

        assertTrue(h.manager.rotateConnection(), "rotation refused")
        delay(50)

        assertEquals(2, h.factory.count, "replacement socket not opened")
        assertFalse(h.factory[0].canceled, "live socket must keep serving while the replacement warms")
        assertEquals(ConnectionStatus.CONNECTED, h.manager.status)
        assertTrue(statuses.isEmpty(), "rotation must not emit a status change: $statuses")
        assertTrue(h.factory[1].actions().contains("auth"), "replacement never authenticated")
        h.stop()
    }

    // MARK: - 2. Resubscribe

    @Test
    fun reIssuesEverySubscriptionOnTheReplacement() = runBlocking {
        val h = harness()
        h.connectAndAuth()
        h.manager.subscribeMids("sim", listOf("hl:0:BTC"))
        h.manager.acquireCandles(listOf("hl:0:BTC"), listOf(network.arca.sdk.models.CandleInterval.ONE_MINUTE))
        h.manager.acquireOI(listOf("hl:0:BTC"), listOf(network.arca.sdk.models.CandleInterval.ONE_MINUTE))
        h.manager.watchPath("/users/alice")
        h.manager.watchChartHistory("/users/alice")
        delay(50)

        assertTrue(h.manager.rotateConnection())
        h.factory[1].deliver(AUTHENTICATED)
        delay(50)

        val actions = h.factory[1].actions()
        assertTrue(actions.contains("subscribe_mids"), "mids not re-issued: $actions")
        assertTrue(actions.contains("subscribe_candles"), "candles not re-issued: $actions")
        assertTrue(actions.contains("subscribe_oi"), "OI not re-issued: $actions")
        assertTrue(actions.contains("watch"), "path watch not re-issued: $actions")
        assertTrue(actions.contains("watch_chart_history"), "chart history not re-issued: $actions")
        assertEquals("ping", actions.last(), "the barrier ping must be queued behind the whole batch")
        h.stop()
    }

    // MARK: - 3. Takeover

    @Test
    fun retiresTheOldSocketAndDeliversFromTheNewOne() = runBlocking {
        val h = harness()
        h.connectAndAuth()
        val mids = h.recordMids()

        assertTrue(h.manager.rotateConnection())
        h.factory[1].deliver(AUTHENTICATED)
        delay(30)
        h.factory[1].deliver(PONG)
        delay(50)

        assertTrue(h.factory[0].canceled, "retired socket was not closed")
        h.factory[1].deliver(midsEvent("101"))
        delay(80)
        assertEquals(listOf("101"), mids.map { it["hl:0:BTC"] })
        h.stop()
    }

    // MARK: - 4. Warming-socket duplicates

    @Test
    fun dropsEventsArrivingOnTheWarmingSocket() = runBlocking {
        val h = harness()
        h.connectAndAuth()
        val mids = h.recordMids()

        assertTrue(h.manager.rotateConnection())
        h.factory[1].deliver(AUTHENTICATED)
        delay(30)
        // The live socket is carrying this same stream, so a copy on the warming
        // socket would be a second dispatch of something consumers already have.
        h.factory[1].deliver(midsEvent("999"))
        delay(80)

        assertTrue(mids.isEmpty(), "warming socket must not dispatch events: $mids")
        h.stop()
    }

    // MARK: - 5. Retired-socket traffic

    @Test
    fun ignoresLateTrafficFromTheRetiredSocket() = runBlocking {
        val h = harness()
        h.connectAndAuth()
        h.promoteRotation()
        val mids = h.recordMids()

        h.factory[0].deliver(midsEvent("777"))
        delay(80)

        assertTrue(mids.isEmpty(), "retired socket must not reach consumers: $mids")
        h.stop()
    }

    // MARK: - 6. Gap detection

    @Test
    fun doesNotReportADeliveryGapAcrossTheHandoff() = runBlocking {
        val h = harness()
        h.connectAndAuth()
        val gaps = Collections.synchronizedList(mutableListOf<Int>())
        h.manager.onGap { gaps.add(it) }

        h.factory[0].deliver(sequencedEvent(5))
        delay(30)
        h.promoteRotation()
        // A new connection is a new sequence space, so the server's counter
        // restarting high must not read as 44 missed frames.
        h.factory[1].deliver(sequencedEvent(50))
        delay(80)

        assertTrue(gaps.isEmpty(), "gap falsely reported across a gapless swap: $gaps")
        h.stop()
    }

    // MARK: - 7. Callbacks

    @Test
    fun firesRotatedButNotAuthenticated() = runBlocking {
        val h = harness()
        h.connectAndAuth()
        val rotated = AtomicInteger(0)
        val authenticated = AtomicInteger(0)
        h.manager.onRotated { rotated.incrementAndGet() }
        h.manager.onAuthenticated { authenticated.incrementAndGet() }

        h.promoteRotation()

        assertEquals(1, rotated.get(), "onRotated did not fire")
        assertEquals(0, authenticated.get(), "a rotation is not a reconnect and must not re-fire onAuthenticated")
        h.stop()
    }

    // MARK: - 8. Replacement failure

    @Test
    fun keepsTheOriginalSocketServingWhenTheReplacementDies() = runBlocking {
        val h = harness()
        h.connectAndAuth()
        val statuses = h.recordStatuses()
        delay(30)
        statuses.clear()
        val mids = h.recordMids()

        assertTrue(h.manager.rotateConnection())
        h.factory[1].fail()
        delay(80)

        assertEquals(ConnectionStatus.CONNECTED, h.manager.status)
        assertTrue(statuses.isEmpty(), "a failed rotation must be invisible to consumers: $statuses")
        assertFalse(h.factory[0].canceled, "live socket was collateral damage")
        h.factory[0].deliver(midsEvent("55"))
        delay(80)
        assertEquals(listOf("55"), mids.map { it["hl:0:BTC"] })
        h.stop()
    }

    // MARK: - 9. Handoff timeout

    @Test
    fun abandonsAReplacementThatNeverCompletesTheHandoff() = runBlocking {
        val h = harness(handoffTimeoutMs = 200)
        h.connectAndAuth()

        assertTrue(h.manager.rotateConnection())
        // Authenticates but never answers the barrier ping.
        h.factory[1].deliver(AUTHENTICATED)
        delay(400)

        assertTrue(h.factory[1].canceled, "half-built replacement was left hanging")
        assertEquals(ConnectionStatus.CONNECTED, h.manager.status)
        assertFalse(h.factory[0].canceled)
        h.stop()
    }

    // MARK: - 10. Refusals

    @Test
    fun refusesToRotateWhileDisconnected() {
        val h = harness()
        assertFalse(h.manager.rotateConnection(), "rotated with no socket to hand off from")
        assertEquals(0, h.factory.count)
        h.stop()
    }

    @Test
    fun doesNotStartASecondHandoffInParallel() = runBlocking {
        val h = harness()
        h.connectAndAuth()

        assertTrue(h.manager.rotateConnection())
        assertFalse(h.manager.rotateConnection(), "second handoff started while the first was in flight")
        delay(50)
        assertEquals(2, h.factory.count)
        h.stop()
    }

    // MARK: - 12/13/14. Scheduling

    @Test
    fun autoRotatesOnTheConfiguredLifetime() = runBlocking {
        // Rotation lands at 0.85 of the lifetime ±10%, i.e. 306-374 ms here.
        val h = harness(connectionLifetimeMs = 400)
        h.connectAndAuth()
        delay(900)

        assertEquals(2, h.factory.count, "no rotation was armed for the configured lifetime")
        assertTrue(h.factory[1].actions().contains("auth"))
        h.stop()
    }

    @Test
    fun doesNotRotateWhenTheLifetimeIsZero() = runBlocking {
        val h = harness(connectionLifetimeMs = 0)
        h.connectAndAuth()
        delay(600)

        assertEquals(1, h.factory.count, "rotation armed despite being disabled")
        h.stop()
    }

    @Test
    fun lifetimeZeroIgnoresTheServerReportedLifetime() = runBlocking {
        // Production advertises a cap, so a server value that overrode an
        // explicit 0 would make the documented opt-out inoperative exactly where
        // it is reached for — during an incident, on the live fleet.
        val h = harness(connectionLifetimeMs = 0)
        h.connectAndAuth("""{"type":"authenticated","maxConnectionLifetimeSec":1}""")
        delay(1_400)

        assertEquals(1, h.factory.count, "a configured 0 must outrank the server's lifetime")
        h.stop()
    }

    @Test
    fun prefersTheServerReportedLifetime() = runBlocking {
        // The client would not rotate for ~51 s; the server says the real cap is 1 s.
        val h = harness(connectionLifetimeMs = 60_000)
        h.connectAndAuth("""{"type":"authenticated","maxConnectionLifetimeSec":1}""")
        delay(1_400)

        assertEquals(2, h.factory.count, "server-reported lifetime was ignored")
        h.stop()
    }

    // MARK: - 15. Aggregation watch

    @Test
    fun reCreatesTheAggregationWatchOnRotationWithoutReconnectingState() = runBlocking {
        val dispatcher = AggregationDispatcher()
        val server = MockWebServer()
        server.dispatcher = dispatcher
        server.start()
        servers.add(server)

        val factory = FakeSocketFactory()
        val baseUrl = server.url("/").toString().trimEnd('/')
        val http = OkHttpClient()
        val ws = WebSocketManager(
            baseUrl = baseUrl,
            token = FAKE_JWT,
            realmId = "rlm_test",
            httpClient = http,
            log = ArcaLogger.disabled,
            connectionLifetimeMs = 0,
            socketFactory = factory,
        )
        managers.add(ws)
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

        val stream = arca.watchAggregation(listOf(AggregationSource(AggregationSourceType.PREFIX, "/users")))
        factory[0].deliver(AUTHENTICATED)
        delay(80)
        assertEquals(1, dispatcher.createCount, "initial watch not created")

        val states = Collections.synchronizedList(mutableListOf<WatchStreamState>())
        val stateJob = launch { stream.state.collect { states.add(it) } }
        delay(50)

        assertTrue(ws.rotateConnection())
        factory[1].deliver(AUTHENTICATED)
        delay(30)
        factory[1].deliver(PONG)
        delay(250)

        // The watch is server state on the connection that just retired; without
        // a re-create the stream would go permanently quiet with no error.
        assertEquals(2, dispatcher.createCount, "aggregation watch not re-created on rotation")
        assertFalse(
            states.contains(WatchStreamState.RECONNECTING),
            "a gapless swap must not surface as a reconnect: $states",
        )
        stateJob.cancel()
        stream.stop()
    }

    // MARK: - Harness

    private fun harness(
        connectionLifetimeMs: Long = 0,
        handoffTimeoutMs: Long = 10_000,
    ): Harness {
        val factory = FakeSocketFactory()
        val manager = WebSocketManager(
            baseUrl = "http://localhost:3052",
            token = "test",
            realmId = "rlm_test",
            httpClient = OkHttpClient(),
            log = ArcaLogger.disabled,
            connectionLifetimeMs = connectionLifetimeMs,
            handoffTimeoutMs = handoffTimeoutMs,
            socketFactory = factory,
        )
        managers.add(manager)
        return Harness(manager, factory)
    }

    private inner class Harness(val manager: WebSocketManager, val factory: FakeSocketFactory) {
        private val jobs = mutableListOf<Job>()

        fun connectAndAuth(authFrame: String = AUTHENTICATED) {
            manager.connect()
            factory[0].deliver(authFrame)
        }

        /** Run a full rotation to completion: warm, resubscribe, barrier pong, swap. */
        suspend fun promoteRotation() {
            assertTrue(manager.rotateConnection(), "rotation refused")
            factory.latest.deliver(AUTHENTICATED)
            delay(30)
            factory.latest.deliver(PONG)
            delay(60)
        }

        /** The bus has `replay = 0`, so collectors must be live before injection. */
        fun recordMids(): List<Map<String, String>> {
            val out = Collections.synchronizedList(mutableListOf<Map<String, String>>())
            jobs += scope().launch { manager.midsEvents().collect { out.add(it) } }
            Thread.sleep(60)
            return out
        }

        fun recordStatuses(): MutableList<ConnectionStatus> {
            val out = Collections.synchronizedList(mutableListOf<ConnectionStatus>())
            jobs += scope().launch { manager.statusStream.collect { out.add(it) } }
            Thread.sleep(60)
            return out
        }

        fun stop() {
            jobs.forEach { it.cancel() }
        }
    }

    private fun scope() = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.Dispatchers.Default)

    private companion object {
        const val AUTHENTICATED = """{"type":"authenticated"}"""
        const val PONG = """{"type":"pong"}"""

        val FAKE_JWT: String = run {
            val enc = Base64.getUrlEncoder().withoutPadding()
            val header = enc.encodeToString("""{"alg":"HS256","typ":"JWT"}""".toByteArray())
            val payload = enc.encodeToString("""{"realmId":"rlm_test","sub":"usr_test"}""".toByteArray())
            "$header.$payload.fakesig"
        }

        fun midsEvent(price: String): String =
            """{"type":"mids.snapshot","mids":{"hl:0:BTC":"$price"}}"""

        fun sequencedEvent(seq: Int): String =
            """{"type":"balance.updated","entityId":"bal_1","deliverySeq":$seq}"""
    }
}

/**
 * A [WebSocket] the test owns outright: frames it "sends" are recorded, and
 * inbound frames are pushed straight at the listener the manager registered for
 * this socket. Nothing is shared between instances, which is what lets a test
 * address the live and the warming socket separately.
 */
internal class FakeWebSocket(
    private val originalRequest: Request,
    private val listener: WebSocketListener,
) : WebSocket {
    private val frames = Collections.synchronizedList(mutableListOf<String>())

    @Volatile
    var canceled: Boolean = false
        private set

    val sent: List<String> get() = synchronized(frames) { frames.toList() }

    /** The `action` field of every frame sent on this socket, in order. */
    fun actions(): List<String> = sent.mapNotNull { ACTION.find(it)?.groupValues?.get(1) }

    override fun request(): Request = originalRequest
    override fun queueSize(): Long = 0
    override fun send(text: String): Boolean {
        frames.add(text)
        return true
    }

    override fun send(bytes: ByteString): Boolean = send(bytes.utf8())
    override fun close(code: Int, reason: String?): Boolean {
        canceled = true
        return true
    }

    override fun cancel() {
        canceled = true
    }

    fun deliver(text: String) {
        listener.onMessage(this, text)
    }

    fun fail(error: Throwable = RuntimeException("socket died")) {
        listener.onFailure(this, error, null)
    }

    private companion object {
        val ACTION = Regex("\"action\"\\s*:\\s*\"([^\"]+)\"")
    }
}

internal class FakeSocketFactory : WebSocket.Factory {
    private val created = Collections.synchronizedList(mutableListOf<FakeWebSocket>())

    val count: Int get() = created.size
    operator fun get(index: Int): FakeWebSocket = created[index]
    val latest: FakeWebSocket get() = synchronized(created) { created.last() }

    override fun newWebSocket(request: Request, listener: WebSocketListener): WebSocket =
        FakeWebSocket(request, listener).also { created.add(it) }
}

private class AggregationDispatcher : Dispatcher() {
    private val creates = AtomicInteger(0)
    val createCount: Int get() = creates.get()

    override fun dispatch(request: RecordedRequest): MockResponse {
        val path = (request.path ?: "").substringBefore("?")
        return when {
            path.endsWith("/aggregations/watch") && request.method == "POST" -> {
                val n = creates.incrementAndGet()
                json("""{"success":true,"data":{"watchId":"wch_$n","aggregation":$AGGREGATION}}""")
            }
            path.contains("/aggregations/watch/") -> json("""{"success":true,"data":{}}""")
            else -> MockResponse().setResponseCode(404)
        }
    }

    private fun json(body: String): MockResponse =
        MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json").setBody(body)

    private companion object {
        const val AGGREGATION =
            """{"prefix":"/users","totalEquityUsd":"1000","departingUsd":"0","breakdown":[]}"""
    }
}
