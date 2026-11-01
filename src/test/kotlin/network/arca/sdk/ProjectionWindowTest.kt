package network.arca.sdk

import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.ProjectedValuation
import network.arca.sdk.models.ProjectionValuationsPage
import network.arca.sdk.models.ProjectionWindow
import network.arca.sdk.models.RealmEvent
import network.arca.sdk.models.WindowedStatus
import network.arca.sdk.models.revalued
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.Base64
import java.util.Collections

/**
 * Windowed projection figures and the projection stream's recovery paths:
 * re-marking moves `equity`, `unrealizedValue` and `pnl` by the position P&L
 * change only (unsettled funding and the baseline stay as delivered, volume
 * never moves), rows decode one by one, a replaced account at the same path
 * survives its frame, a delivery gap or server resync re-reads the scope
 * under the same watch, and a failed re-create is retried.
 */
class ProjectionWindowTest {

    private val managers = Collections.synchronizedList(mutableListOf<WebSocketManager>())
    private val servers = Collections.synchronizedList(mutableListOf<MockWebServer>())

    @AfterEach
    fun tearDown() {
        synchronized(managers) { managers.toList() }.forEach { runCatching { it.shutdown() } }
        synchronized(servers) { servers.toList() }.forEach { runCatching { it.shutdown() } }
    }

    // MARK: - Re-marking

    @Test
    fun remarkKeepsUnsettledFunding() {
        // equity 1047.5 = realized 1000 + position P&L 50 - unsettled funding 2.5
        val v = row(
            """{"objectId":"obj_a","path":"/users/alice","type":"exchange",
                "equity":"1047.5","realizedValue":"1000","unrealizedValue":"50",
                "positions":[{"market":"hl:0:BTC","side":"long","size":"1","entryPrice":"100",
                              "markPrice":"150","unrealizedPnl":"50","valueUsd":"50"}]}""",
        )
        val out = v.revalued(mapOf("hl:0:BTC" to "160"))
        assertEquals("1057.5", out.equity, "funding owed stays owed when the mark moves")
        assertEquals("60", out.unrealizedValue)
        assertEquals("1000", out.realizedValue)
    }

    @Test
    fun remarkShiftsWindowedPnlButNeverVolume() {
        val v = row(
            """{"objectId":"obj_a","path":"/users/alice","type":"exchange","equity":"1000",
                "positions":[{"market":"hl:0:ETH","side":"long","size":"2","entryPrice":"100",
                              "markPrice":"100","unrealizedPnl":"0","valueUsd":"0"}],
                "pnl":${pnl("12.5")},"volume":${volume("3000")}}""",
        )
        val out = v.revalued(mapOf("hl:0:ETH" to "110"))
        assertEquals("32.5", out.pnl?.pnlUsd)
        assertEquals(FROM, out.pnl?.from)
        assertEquals("3000", out.volume?.volumeUsd, "a price move is not volume")
        assertEquals("1020", out.equity)
    }

    @Test
    fun remarkIsExactDecimal() {
        val v = row(
            """{"objectId":"obj_a","path":"/users/alice","type":"exchange","equity":"100.1","unrealizedValue":"0",
                "positions":[{"market":"hl:0:XRP","side":"long","size":"3","entryPrice":"0.1",
                              "markPrice":"0.1","unrealizedPnl":"0","valueUsd":"0"}]}""",
        )
        val out = v.revalued(mapOf("hl:0:XRP" to "0.3"))
        assertEquals("0.6", out.unrealizedValue)
        assertEquals("100.7", out.equity)
    }

    @Test
    fun pendingPnlIsNotGivenANumber() {
        val v = row(
            """{"objectId":"obj_a","path":"/users/alice","type":"exchange","equity":"1000",
                "positions":[{"market":"hl:0:BTC","side":"long","size":"1","entryPrice":"100",
                              "markPrice":"100","unrealizedPnl":"0","valueUsd":"0"}],
                "pnl":{"window":"30d","from":"$FROM","status":"pending"},
                "volume":{"window":"30d","from":"$FROM","status":"pending"}}""",
        )
        val out = v.revalued(mapOf("hl:0:BTC" to "200"))
        assertNull(out.pnl?.pnlUsd, "pending P&L must not read as zero")
        assertEquals(WindowedStatus.PENDING, out.pnl?.windowedStatus)
        assertNull(out.volume?.volumeUsd)
        assertEquals("1100", out.equity)
    }

    @Test
    fun serverPricedRowIsNotRemarked() {
        val v = row(
            """{"objectId":"obj_a","path":"/users/alice","type":"exchange","equity":"1000","pricingMode":"server",
                "positions":[{"market":"hl:0:BTC","side":"long","size":"1","entryPrice":"100",
                              "markPrice":"100","unrealizedPnl":"0","valueUsd":"0"}],
                "pnl":${pnl("5")}}""",
        )
        assertEquals(v, v.revalued(mapOf("hl:0:BTC" to "200")))
    }

    // MARK: - Decoding

    @Test
    fun pageDecodesRowByRow() {
        val page = arcaJson.decodeFromString(
            ProjectionValuationsPage.serializer(),
            """{"projection":"board","fields":["equity"],"window":"30d","valuations":[
                {"objectId":"obj_a","path":"/users/alice","type":"exchange","equity":"1"},
                {"objectId":"obj_b","type":"exchange","equity":"2"},
                {"objectId":"obj_c","path":"/users/carol","type":"exchange","equity":"3",
                 "pnl":{"window":"30d","from":"$FROM","status":"incomplete","coverage":"since_creation",
                        "coveredFrom":"2026-10-20T00:00:00Z","pnlUsd":"4"}}]}""",
        )
        assertEquals("30d", page.window)
        assertEquals(listOf("/users/alice", "/users/carol"), page.valuations.map { it.path }, "the unreadable row is skipped, not the page")
        assertEquals(WindowedStatus.INCOMPLETE, page.valuations.last().pnl?.windowedStatus)
        assertEquals("2026-10-20T00:00:00Z", page.valuations.last().pnl?.coveredFrom)
    }

    @Test
    fun deltaFrameWithAnUnreadableRowKeepsTheRest() {
        val event = arcaJson.decodeFromString(
            RealmEvent.serializer(),
            """{"type":"object.valuation","watchId":"w1","projection":"board","valuations":{
                "/users/alice":${windowedRow("/users/alice", "obj_a", "1", "1")},
                "/users/bob":{"objectId":"obj_b","type":"exchange"}}}""",
        )
        assertEquals(setOf("/users/alice"), event.valuations?.keys)
    }

    @Test
    fun unreadableFigureDropsOnlyTheFigure() {
        val v = row(
            """{"objectId":"obj_a","path":"/users/alice","type":"exchange","equity":"1",
                "pnl":{"window":"30d"},"volume":${volume("5")}}""",
        )
        assertNull(v.pnl)
        assertEquals("5", v.volume?.volumeUsd)
    }

    // MARK: - Stream

    @Test
    fun watchSendsTheWindowAndRowsCarryFigures() = runBlocking {
        val h = arcaHarness()
        val stream = open(h)
        val request = h.factory[0].lastFrame("watch_projection")
        assertEquals("30d", request.string("window"))
        assertEquals("30d", stream.window)
        assertEquals("1", stream.valuations.value["/users/alice"]?.pnl?.pnlUsd)
        assertEquals("10", stream.valuations.value["/users/alice"]?.volume?.volumeUsd)
        assertFalse(stream.isStale)
        stream.stop()
    }

    @Test
    fun replacedAccountAtTheSamePathIsKept() = runBlocking {
        val h = arcaHarness()
        val stream = open(h)
        h.factory[0].deliver(
            """{"type":"object.valuation","watchId":"w1","projection":"board","deliverySeq":1,
                "removed":["/users/alice"],
                "valuations":{"/users/alice":${windowedRow("/users/alice", "obj_new", "0", "0")}}}""",
        )
        eventually { stream.valuations.value["/users/alice"]?.objectId?.value == "obj_new" }
        assertEquals(1, stream.valuations.value.size)
        stream.stop()
    }

    @Test
    fun deliveryGapReReadsUnderTheSameWatch() = runBlocking {
        val h = arcaHarness()
        val stream = open(h)
        val socket = h.factory[0]

        socket.deliver(frame(seq = 1, pnl = "2"))
        eventually { stream.valuations.value["/users/alice"]?.pnl?.pnlUsd == "2" }
        socket.deliver(frame(seq = 4, pnl = "3"))

        eventually { socket.actions().contains("update_projection_watch") }
        val request = socket.lastFrame("update_projection_watch")
        assertEquals("w1", request.string("watchId"), "a gap re-reads under the same watch")
        assertEquals(listOf("/users/alice"), request["paths"]?.jsonArray?.map { it.jsonPrimitive.content })
        assertTrue(stream.isStale)
        socket.deliver(reply("projection_watch_updated", request.string("requestId"), "w1", windowedRow("/users/alice", "obj_a", "7", "70")))
        eventually { stream.valuations.value["/users/alice"]?.pnl?.pnlUsd == "7" && !stream.isStale }
        assertEquals(1, socket.actions().count { it == "watch_projection" }, "no new watch for a gap")
        stream.stop()
    }

    @Test
    fun serverResyncReReadsTheScope() = runBlocking {
        val h = arcaHarness()
        val stream = open(h)
        val socket = h.factory[0]

        socket.deliver("""{"type":"stream.resync","deliverySeq":1}""")
        eventually { socket.actions().contains("update_projection_watch") }
        val request = socket.lastFrame("update_projection_watch")
        socket.deliver(reply("projection_watch_updated", request.string("requestId"), "w1", windowedRow("/users/alice", "obj_a", "9", "90")))
        eventually { stream.valuations.value["/users/alice"]?.volume?.volumeUsd == "90" }
        stream.stop()
    }

    @Test
    fun failedRecreateIsRetriedWithTheLastRowsKept() = runBlocking {
        val h = arcaHarness()
        val stream = open(h)

        h.factory[0].fail()
        eventually { stream.state.value == WatchStreamState.RECONNECTING }
        eventually(3_000) { h.factory.count == 2 }
        val next = h.factory[1]
        next.deliver(AUTHENTICATED)
        eventually { next.actions().contains("watch_projection") }
        val first = next.lastFrame("watch_projection")
        assertEquals("30d", first.string("window"), "the re-create keeps the window")
        next.deliver("""{"type":"error","requestId":"${first.string("requestId")}","message":"busy"}""")

        delay(100)
        assertTrue(stream.isStale)
        assertEquals("1", stream.valuations.value["/users/alice"]?.pnl?.pnlUsd, "the last rows stay while retrying")

        eventually(4_000) { next.actions().count { it == "watch_projection" } >= 2 }
        val second = next.lastFrame("watch_projection")
        next.deliver(reply("projection_watch_created", second.string("requestId"), "w2", windowedRow("/users/alice", "obj_a", "5", "50")))
        eventually { stream.watchId.value == "w2" && !stream.isStale }
        assertEquals("5", stream.valuations.value["/users/alice"]?.pnl?.pnlUsd)
        stream.stop()
    }

    // MARK: - Helpers

    private fun row(json: String): ProjectedValuation = arcaJson.decodeFromString(ProjectedValuation.serializer(), json)

    private suspend fun open(h: ArcaHarness): ProjectionWatchStream = kotlinx.coroutines.coroutineScope {
        val pending = async { h.arca.watchProjection("board", paths = listOf("/users/alice"), window = ProjectionWindow.THIRTY_DAYS) }
        eventually { h.factory.count >= 1 }
        h.factory[0].deliver(AUTHENTICATED)
        eventually { h.factory[0].actions().contains("watch_projection") }
        val request = h.factory[0].lastFrame("watch_projection")
        h.factory[0].deliver(reply("projection_watch_created", request.string("requestId"), "w1", windowedRow("/users/alice", "obj_a", "1", "10")))
        withTimeout(5_000) { pending.await() }
    }

    private class ArcaHarness(val arca: Arca, val factory: FakeSocketFactory)

    private fun arcaHarness(): ArcaHarness {
        val server = MockWebServer().apply {
            dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse = MockResponse().setResponseCode(404)
            }
            start()
        }
        servers.add(server)
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
            client = ArcaClient(token = FAKE_JWT, baseUrl = server.url("/").toString().trimEnd('/'), httpClient = http, logger = ArcaLogger.disabled),
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

    private fun JsonObject.string(key: String): String = this[key]?.jsonPrimitive?.content ?: ""

    private companion object {
        const val AUTHENTICATED = """{"type":"authenticated"}"""
        const val FROM = "2026-10-01T00:00:00Z"

        fun pnl(value: String): String =
            """{"window":"30d","from":"$FROM","asOf":"2026-10-31T00:00:00Z","coverage":"full","status":"complete","pnlUsd":"$value"}"""

        fun volume(value: String): String =
            """{"window":"30d","from":"$FROM","asOf":"2026-10-31T00:00:00Z","coverage":"full","status":"complete","volumeUsd":"$value"}"""

        fun windowedRow(path: String, objectId: String, pnl: String, volume: String): String =
            """{"objectId":"$objectId","path":"$path","type":"exchange","equity":"1000","positions":[],""" +
                """"pnl":${pnl(pnl)},"volume":${volume(volume)}}"""

        fun frame(seq: Int, pnl: String): String =
            """{"type":"object.valuation","watchId":"w1","projection":"board","deliverySeq":$seq,""" +
                """"valuations":{"/users/alice":${windowedRow("/users/alice", "obj_a", pnl, "10")}}}"""

        fun reply(type: String, requestId: String, watchId: String, vararg rows: String): String =
            """{"type":"$type","requestId":"$requestId","watchId":"$watchId","projection":"board",""" +
                """"fields":["equity","positions","pnl","volume"],"window":"30d","valuations":[${rows.joinToString(",")}]}"""

        val FAKE_JWT: String = run {
            val enc = Base64.getUrlEncoder().withoutPadding()
            val header = enc.encodeToString("""{"alg":"HS256","typ":"JWT"}""".toByteArray())
            val payload = enc.encodeToString("""{"realmId":"rlm_test","sub":"usr_test"}""".toByteArray())
            "$header.$payload.fakesig"
        }
    }
}
