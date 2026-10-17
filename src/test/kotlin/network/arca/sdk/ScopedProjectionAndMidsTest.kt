package network.arca.sdk

import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import network.arca.sdk.internal.arcaJson
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.util.Base64
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue

/**
 * A projection watch is scoped to the accounts the client names, and the Arca
 * mids subscription is scoped to the markets the client's consumers declare.
 * These pin the wire shape (paths / prefix on `watch_projection`,
 * `update_projection_watch` under the same watch id, an empty scope sent as
 * exactly that), the reconnect contract (re-create with the CURRENT scope),
 * and that `subscribe_mids` carries the union of declared interests — never
 * `coins: []` on behalf of a consumer that named its markets.
 */
class ScopedProjectionAndMidsTest {
    private val managers = mutableListOf<WebSocketManager>()
    private val servers = mutableListOf<MockWebServer>()

    @AfterEach
    fun tearDown() {
        managers.forEach { it.disconnect() }
        servers.forEach { it.shutdown() }
    }

    // MARK: - Interest-scoped Arca mids

    @Test
    fun arcaMidsSubscriptionCarriesDeclaredInterests() = runBlocking {
        val h = arcaHarness()
        h.connectAndAuth()

        val btc = async { h.arca.watchPrices(markets = listOf("hl:0:BTC")) }
        eventually { h.socket.actions().contains("subscribe_mids") }
        assertEquals(listOf("hl:0:BTC"), h.socket.lastMidsCoins())
        h.socket.deliver("""{"type":"mids.snapshot","mids":{"hl:0:BTC":"80000"}}""")
        val btcStream = btc.await()

        val eth = async { h.arca.watchPrices(markets = listOf("hl:0:ETH")) }
        eventually { h.socket.lastMidsCoins() == listOf("hl:0:BTC", "hl:0:ETH") }
        h.socket.deliver("""{"type":"mids.snapshot","mids":{"hl:0:BTC":"80000","hl:0:ETH":"3000"}}""")
        val ethStream = eth.await()
        eventually { ethStream.prices.value["hl:0:ETH"] == "3000" }

        // Dropping a consumer narrows the set after the release debounce.
        ethStream.stop()
        eventually(3_000) { h.socket.lastMidsCoins() == listOf("hl:0:BTC") }

        btcStream.stop()
        eventually(3_000) { h.socket.actions().contains("unsubscribe_mids") }
    }

    @Test
    fun setMarketsResendsTheArcaSubscription() = runBlocking {
        val h = arcaHarness()
        h.connectAndAuth()
        val pending = async { h.arca.watchPrices(markets = listOf("hl:0:BTC")) }
        eventually { h.socket.actions().contains("subscribe_mids") }
        h.socket.deliver("""{"type":"mids.snapshot","mids":{"hl:0:BTC":"80000"}}""")
        val stream = pending.await()

        stream.setMarkets(listOf("hl:0:SOL", "hl:0:BTC"))
        eventually { h.socket.lastMidsCoins() == listOf("hl:0:BTC", "hl:0:SOL") }
        stream.stop()
    }

    @Test
    fun unscopedWatchPricesStillAsksForEveryMarket() = runBlocking {
        val h = arcaHarness()
        h.connectAndAuth()
        val pending = async { h.arca.watchPrices() }
        eventually { h.socket.actions().contains("subscribe_mids") }
        assertEquals(emptyList<String>(), h.socket.lastMidsCoins())
        h.socket.deliver("""{"type":"mids.snapshot","mids":{"hl:0:BTC":"80000"}}""")
        pending.await().stop()
    }

    @Test
    fun reconnectResubscribesWithTheInterestSet() = runBlocking {
        val h = arcaHarness()
        h.connectAndAuth()
        val pending = async { h.arca.watchPrices(markets = listOf("hl:0:BTC")) }
        eventually { h.socket.actions().contains("subscribe_mids") }
        h.socket.deliver("""{"type":"mids.snapshot","mids":{"hl:0:BTC":"80000"}}""")
        val stream = pending.await()

        h.socket.fail()
        eventually(5_000) { h.factory.count >= 2 }
        val next = h.factory[1]
        next.deliver(AUTHENTICATED)
        eventually(3_000) { next.actions().contains("subscribe_mids") }
        assertEquals(listOf("hl:0:BTC"), next.lastMidsCoins())
        stream.stop()
    }

    // MARK: - Scoped projection watch

    @Test
    fun watchProjectionSendsPathsAndSubscribesToRowMarkets() = runBlocking {
        val h = arcaHarness()
        h.connectAndAuth()
        val pending = async { h.arca.watchProjection("board", paths = listOf("/users/bob", "/users/alice")) }
        eventually { h.socket.actions().contains("watch_projection") }
        val request = h.socket.lastFrame("watch_projection")
        assertEquals(listOf("/users/bob", "/users/alice"), request["paths"]!!.jsonArray.map { it.jsonPrimitive.content })
        assertNull(request["prefix"])

        h.socket.deliver(projectionReply("projection_watch_created", request.string("requestId"), "w1", listOf(
            row("/users/alice", "1000", listOf("hl:0:BTC")),
            row("/users/bob", "2000", listOf("hl:0:ETH", "hl:0:BTC")),
        )))
        val stream = awaitOrFail(pending)
        assertEquals(setOf("/users/alice", "/users/bob"), stream.valuations.value.keys)
        assertEquals(ProjectionWatchScope(paths = listOf("/users/bob", "/users/alice")), stream.scope.value)

        // The stream's mids interest is its rows' markets, not every market.
        eventually { h.socket.lastMidsCoins() == listOf("hl:0:BTC", "hl:0:ETH") }
        stream.stop()
    }

    @Test
    fun noScopeIsSentAsNoScope() = runBlocking {
        val h = arcaHarness()
        h.connectAndAuth()
        val pending = async { h.arca.watchProjection("board") }
        eventually { h.socket.actions().contains("watch_projection") }
        val request = h.socket.lastFrame("watch_projection")
        assertNull(request["paths"], "an empty scope is an empty watch, not everyone")
        assertNull(request["prefix"])
        h.socket.deliver(projectionReply("projection_watch_created", request.string("requestId"), "w1", emptyList()))
        val stream = awaitOrFail(pending)
        assertTrue(stream.valuations.value.isEmpty())
        assertTrue(stream.scope.value.isEmpty)
        stream.stop()
    }

    @Test
    fun setPathsUpdatesInPlaceAndReplacesRows() = runBlocking {
        val h = arcaHarness()
        h.connectAndAuth()
        val pending = async { h.arca.watchProjection("board", paths = listOf("/users/alice")) }
        eventually { h.socket.actions().contains("watch_projection") }
        val created = h.socket.lastFrame("watch_projection")
        h.socket.deliver(projectionReply("projection_watch_created", created.string("requestId"), "w1", listOf(
            row("/users/alice", "1000", listOf("hl:0:BTC")),
        )))
        val stream = awaitOrFail(pending)

        val update = async { stream.setPaths(listOf("/users/carol")) }
        eventually { h.socket.actions().contains("update_projection_watch") }
        val request = h.socket.lastFrame("update_projection_watch")
        assertEquals("w1", request.string("watchId"))
        assertEquals(listOf("/users/carol"), request["paths"]!!.jsonArray.map { it.jsonPrimitive.content })
        h.socket.deliver(projectionReply("projection_watch_updated", request.string("requestId"), "w1", listOf(
            row("/users/carol", "3000", listOf("hl:0:SOL")),
        )))
        awaitOrFail(update)

        assertEquals("w1", stream.watchId.value, "an update keeps the watch id")
        assertEquals(setOf("/users/carol"), stream.valuations.value.keys, "the new scope's snapshot replaces the rows")
        assertEquals(ProjectionWatchScope(paths = listOf("/users/carol")), stream.scope.value)
        eventually(3_000) { h.socket.lastMidsCoins() == listOf("hl:0:SOL") }
        stream.stop()
    }

    @Test
    fun reconnectRecreatesTheWatchWithTheCurrentScope() = runBlocking {
        val h = arcaHarness()
        h.connectAndAuth()
        val pending = async { h.arca.watchProjection("board", paths = listOf("/users/alice")) }
        eventually { h.socket.actions().contains("watch_projection") }
        val created = h.socket.lastFrame("watch_projection")
        h.socket.deliver(projectionReply("projection_watch_created", created.string("requestId"), "w1", listOf(
            row("/users/alice", "1000", emptyList()),
        )))
        val stream = awaitOrFail(pending)

        val update = async { stream.setPaths(listOf("/users/bob")) }
        eventually { h.socket.actions().contains("update_projection_watch") }
        val request = h.socket.lastFrame("update_projection_watch")
        h.socket.deliver(projectionReply("projection_watch_updated", request.string("requestId"), "w1", listOf(
            row("/users/bob", "2000", emptyList()),
        )))
        awaitOrFail(update)

        h.socket.fail()
        eventually(5_000) { h.factory.count >= 2 }
        val next = h.factory[1]
        next.deliver(AUTHENTICATED)
        eventually(3_000) { next.actions().contains("watch_projection") }
        val recreated = next.lastFrame("watch_projection")
        assertEquals(listOf("/users/bob"), recreated["paths"]!!.jsonArray.map { it.jsonPrimitive.content },
            "re-create uses the scope as updated, not as first opened")
        stream.stop()
    }

    private suspend fun <T> awaitOrFail(deferred: kotlinx.coroutines.Deferred<T>): T =
        withTimeoutOrNull(5_000) { deferred.await() } ?: throw AssertionError("deferred never resolved")

    // MARK: - Harness

    private class ArcaHarness(val arca: Arca, val factory: FakeSocketFactory) {
        val socket: FakeWebSocket get() = factory[0]
        fun connectAndAuth() {
            arca.ws.connect()
            factory[0].deliver(AUTHENTICATED)
        }
    }

    private fun arcaHarness(): ArcaHarness {
        val server = MockWebServer().apply { start() }
        servers.add(server)
        val baseUrl = server.url("/").toString().trimEnd('/')
        val http = OkHttpClient()
        val factory = FakeSocketFactory()
        val ws = WebSocketManager(
            baseUrl = "http://localhost:3052",
            token = "test",
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
        return ArcaHarness(arca, factory)
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

    private fun FakeWebSocket.lastMidsCoins(): List<String>? =
        sent.map { arcaJson.parseToJsonElement(it).jsonObject }
            .lastOrNull { it["action"]?.jsonPrimitive?.content == "subscribe_mids" }
            ?.get("coins")?.jsonArray?.map { it.jsonPrimitive.content }

    private fun JsonObject.string(key: String): String = this[key]?.jsonPrimitive?.content ?: ""

    private fun row(path: String, equity: String, markets: List<String>): String {
        val positions = markets.joinToString(",") {
            """{"market": "$it", "side": "long", "size": "1", "entryPrice": "100", "markPrice": "100", "unrealizedPnl": "0", "valueUsd": "0"}"""
        }
        return """{"objectId": "obj_${path.hashCode()}", "path": "$path", "type": "exchange", "equity": "$equity", "positions": [$positions]}"""
    }

    private fun projectionReply(type: String, requestId: String, watchId: String, rows: List<String>): String =
        """{"type": "$type", "requestId": "$requestId", "watchId": "$watchId", "projection": "board",""" +
            """ "fields": ["equity", "positions"], "valuations": [${rows.joinToString(",")}]}"""

    private companion object {
        const val AUTHENTICATED = """{"type":"authenticated"}"""
        val FAKE_JWT: String = run {
            val enc = Base64.getUrlEncoder().withoutPadding()
            val header = enc.encodeToString("""{"alg":"HS256","typ":"JWT"}""".toByteArray())
            val payload = enc.encodeToString("""{"realmId":"rlm_test","sub":"usr_test"}""".toByteArray())
            "$header.$payload.fakesig"
        }
    }
}
