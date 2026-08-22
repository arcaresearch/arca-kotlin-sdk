package network.arca.sdk

import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.PositionValue
import network.arca.sdk.models.PricingMode
import network.arca.sdk.models.ProjectedValuation
import network.arca.sdk.models.RealmEvent
import network.arca.sdk.models.revalued
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger

/**
 * Projection support: delta-frame merging (changed rows only), removed-path
 * handling, other-watch isolation, client-side re-marking on mids ticks,
 * initial-snapshot pagination over REST, and the `ProjectedValuation.revalued`
 * math — ports of the TypeScript `projection-stream.test.ts` cases.
 */
class ProjectionWatchTest {

    private lateinit var server: MockWebServer
    private lateinit var dispatcher: ProjectionDispatcher

    @BeforeEach
    fun setUp() {
        dispatcher = ProjectionDispatcher()
        server = MockWebServer()
        server.dispatcher = dispatcher
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    // MARK: - Decode

    @Test
    fun realmEventDecodesProjectionDeltaFrame() {
        val event = arcaJson.decodeFromString(RealmEvent.serializer(), DELTA_FRAME)
        assertEquals("object.valuation", event.type)
        assertEquals("watch_1", event.watchId)
        assertEquals("leaderboard", event.projection)
        val vals = event.valuations
        assertNotNull(vals)
        assertEquals(1, vals!!.size)
        assertEquals("2100", vals["/users/alice"]?.equity)
        assertEquals(listOf("/users/bob"), event.removed)
    }

    // MARK: - Revalue math

    @Test
    fun revaluedRecomputesEquityFromRealizedPlusPnl() {
        val row = projected(
            path = "/users/alice", equity = "1010", realizedValue = "1000", unrealizedValue = "10",
            positions = listOf(position(entryPrice = "100", unrealizedPnl = "10")),
        )
        val out = row.revalued(mapOf("hl:0:BTC" to "120"))
        // pnl = 1 * (120 - 100) = 20; equity = realized 1000 + 20
        assertEquals("1020", out.equity)
        assertEquals("20", out.unrealizedValue)
        assertEquals("120", out.positions?.first()?.markPrice)
    }

    @Test
    fun revaluedShiftsEquityByPnlDeltaWithoutRealized() {
        val row = projected(
            path = "/users/alice", equity = "1010", realizedValue = null, unrealizedValue = null,
            positions = listOf(position(entryPrice = "100", unrealizedPnl = "10")),
        )
        val out = row.revalued(mapOf("hl:0:BTC" to "120"))
        // old pnl 10 → new pnl 20; equity shifts by +10
        assertEquals("1020", out.equity)
        assertNull(out.unrealizedValue, "unrealizedValue not projected, must stay absent")
    }

    @Test
    fun revaluedWithoutPositionsIsIdentity() {
        val row = projected(path = "/users/alice", equity = "500", realizedValue = "500", unrealizedValue = null, positions = null)
        assertEquals(row, row.revalued(mapOf("hl:0:BTC" to "999")))
    }

    @Test
    fun revaluedServerPricedRowIsIdentity() {
        val row = projected(
            path = "/users/alice", equity = "1010", realizedValue = "1000", unrealizedValue = "10",
            positions = listOf(position(entryPrice = "100", unrealizedPnl = "10")),
            pricingMode = PricingMode.SERVER,
        )
        assertEquals(row, row.revalued(mapOf("hl:0:BTC" to "120")))
    }

    // MARK: - Stream behavior

    @Test
    fun mergesDeltaAndDropsRemoved() = runBlocking {
        val arca = makeArca()
        val stream = startStream(arca)

        assertEquals(setOf("/users/alice", "/users/bob"), stream.valuations.value.keys)
        assertEquals("watch_1", stream.watchId.value)

        val seen = async {
            withTimeoutOrNull(2_000) {
                stream.valuations.first { it["/users/alice"]?.equity == "2100" }
            }
        }
        delay(150)
        arca.ws.injectMessage(DELTA_FRAME)

        val merged = seen.await()
        assertNotNull(merged, "delta frame never applied")
        // Changed row replaced, removed row dropped, nothing else touched.
        assertEquals(setOf("/users/alice"), merged!!.keys)
        assertEquals("2100", merged["/users/alice"]?.equity)

        stream.stop()
        arca.close()
    }

    @Test
    fun ignoresFramesForOtherWatchIds() = runBlocking {
        val arca = makeArca()
        val stream = startStream(arca)

        arca.ws.injectMessage(DELTA_FRAME.replace("watch_1", "watch_OTHER"))
        delay(250)

        // Both original rows intact, no merge happened.
        assertEquals(setOf("/users/alice", "/users/bob"), stream.valuations.value.keys)
        assertEquals("1010", stream.valuations.value["/users/alice"]?.equity)

        stream.stop()
        arca.close()
    }

    @Test
    fun remarksOnMidsTick() = runBlocking {
        val arca = makeArca()
        val stream = startStream(arca)

        val seen = async {
            withTimeoutOrNull(2_000) {
                stream.valuations.first { it["/users/alice"]?.equity == "1020" }
            }
        }
        delay(150)
        // alice holds 1 BTC long from 100 with realized 1000; mid 120 → equity 1020.
        arca.ws.injectMessage("""{"type":"mids.updated","mids":{"hl:0:BTC":"120"}}""")

        val remarked = seen.await()
        assertNotNull(remarked, "mids tick never re-marked the rows")
        assertEquals("20", remarked!!["/users/alice"]?.unrealizedValue)
        // bob has no positions: untouched by the tick.
        assertEquals("500", remarked["/users/bob"]?.equity)

        stream.stop()
        arca.close()
    }

    @Test
    fun paginatesInitialSnapshotOverRest() = runBlocking {
        val arca = makeArca()
        // Watch reply carries page 1 (alice) + cursor; page 2 (bob) comes from REST.
        val stream = startStream(arca, createdJson = WATCH_CREATED_PAGED)

        assertEquals(setOf("/users/alice", "/users/bob"), stream.valuations.value.keys)
        assertEquals(1, dispatcher.valuationsRequestCount, "exactly one REST page fetch expected")

        stream.stop()
        arca.close()
    }

    // MARK: - Helpers

    private fun makeArca(): Arca = Arca(token = fakeJwt(), baseUrl = server.url("/").toString().trimEnd('/'))

    /**
     * Start a projection watch against the mock server: the request/reply is
     * satisfied by injecting the server's `projection_watch_created` reply for
     * the deterministic first request id.
     */
    private suspend fun startStream(arca: Arca, createdJson: String = WATCH_CREATED): ProjectionWatchStream =
        coroutineScope {
            val pending = async { arca.watchProjection("leaderboard") }
            delay(200)
            arca.ws.injectMessage(createdJson)
            val stream = withTimeoutOrNull(5_000) { pending.await() }
            assertTrue(stream != null, "watchProjection never resolved")
            stream!!
        }

    private fun fakeJwt(): String {
        val enc = Base64.getUrlEncoder().withoutPadding()
        val header = enc.encodeToString("""{"alg":"HS256","typ":"JWT"}""".toByteArray())
        val payload = enc.encodeToString("""{"realmId":"rlm_test","sub":"usr_test"}""".toByteArray())
        return "$header.$payload.fakesig"
    }

    private fun projected(
        path: String,
        equity: String?,
        realizedValue: String?,
        unrealizedValue: String?,
        positions: List<PositionValue>?,
        pricingMode: PricingMode? = null,
    ): ProjectedValuation = ProjectedValuation(
        objectId = ObjectId("obj_1"),
        path = path,
        type = "exchange",
        pricingMode = pricingMode,
        equity = equity,
        realizedValue = realizedValue,
        unrealizedValue = unrealizedValue,
        positions = positions,
    )

    private fun position(entryPrice: String, unrealizedPnl: String): PositionValue = PositionValue(
        market = "hl:0:BTC",
        side = "long",
        size = "1",
        entryPrice = entryPrice,
        markPrice = "110",
        unrealizedPnl = unrealizedPnl,
        valueUsd = unrealizedPnl,
    )

    private companion object {
        const val ALICE_ROW = """
            {"objectId":"obj_a","path":"/users/alice","type":"exchange","equity":"1010","realizedValue":"1000","unrealizedValue":"10",
             "positions":[{"market":"hl:0:BTC","side":"long","size":"1","entryPrice":"100","markPrice":"110","unrealizedPnl":"10","valueUsd":"10"}]}
        """

        const val BOB_ROW = """
            {"objectId":"obj_b","path":"/users/bob","type":"exchange","equity":"500","realizedValue":"500"}
        """

        val WATCH_CREATED = """
            {"type":"projection_watch_created","requestId":"proj-req-1","watchId":"watch_1","projection":"leaderboard",
             "fields":["equity","realizedValue","unrealizedValue","positions"],
             "valuations":[$ALICE_ROW,$BOB_ROW]}
        """.trimIndent()

        val WATCH_CREATED_PAGED = """
            {"type":"projection_watch_created","requestId":"proj-req-1","watchId":"watch_1","projection":"leaderboard",
             "fields":["equity","realizedValue","unrealizedValue","positions"],
             "valuations":[$ALICE_ROW],"cursor":"page2"}
        """.trimIndent()

        val DELTA_FRAME = """
            {"type":"object.valuation","watchId":"watch_1","projection":"leaderboard",
             "valuations":{"/users/alice":{"objectId":"obj_a","path":"/users/alice","type":"exchange","equity":"2100","realizedValue":"2000","unrealizedValue":"100"}},
             "removed":["/users/bob"]}
        """.trimIndent()
    }
}

private class ProjectionDispatcher : Dispatcher() {
    private val valuationsCount = AtomicInteger(0)
    val valuationsRequestCount: Int get() = valuationsCount.get()

    override fun dispatch(request: RecordedRequest): MockResponse {
        val path = (request.path ?: "").substringBefore("?")
        return when {
            path.endsWith("/projections/leaderboard/valuations") -> {
                valuationsCount.incrementAndGet()
                json(VALUATIONS_PAGE_2)
            }
            else -> MockResponse().setResponseCode(404)
        }
    }

    private fun json(body: String): MockResponse =
        MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json").setBody(body)

    private companion object {
        val VALUATIONS_PAGE_2 = """
            {"success":true,"data":{
              "projection":"leaderboard",
              "fields":["equity","realizedValue","unrealizedValue","positions"],
              "valuations":[{"objectId":"obj_b","path":"/users/bob","type":"exchange","equity":"500","realizedValue":"500"}]
            }}
        """.trimIndent()
    }
}
