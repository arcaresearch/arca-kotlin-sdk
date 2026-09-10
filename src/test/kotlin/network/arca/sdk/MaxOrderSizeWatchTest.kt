package network.arca.sdk

import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import network.arca.sdk.models.OrderSide
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger

/**
 * Integration tests for `watchMaxOrderSize` dynamic-MMR resolution. The pure
 * `deriveActiveAssetData` is covered in [ActiveAssetDerivationTest]; here we
 * exercise the watch wiring: that the stream fetches the per-asset MMR once,
 * surfaces it (not the 0.03 fallback), persists it across price recomputes, and
 * defers to the server in server-pricing mode.
 */
class MaxOrderSizeWatchTest {

    private lateinit var server: MockWebServer
    private lateinit var dispatcher: MaxOrderSizeDispatcher

    @BeforeEach
    fun setUp() {
        dispatcher = MaxOrderSizeDispatcher()
        server = MockWebServer()
        server.dispatcher = dispatcher
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    @Test
    fun autoFetchesDynamicMaintenanceMarginRateWhenOmitted() = runBlocking {
        dispatcher.maintenanceMarginRate = "0.01"
        val arca = makeArca()
        val stream = launchWatch(arca, mmr = null)

        assertEquals("0.01", stream.activeAssetData.value?.maintenanceMarginRate)
        assertEquals(1, dispatcher.activeAssetDataRequestCount)

        stream.stop()
        arca.close()
    }

    @Test
    fun honorsExplicitMaintenanceMarginRateButStillResolvesTiersAndSpread() = runBlocking {
        dispatcher.maintenanceMarginRate = "0.01"
        val arca = makeArca()
        val stream = launchWatch(arca, mmr = "0.005")

        assertEquals("0.005", stream.activeAssetData.value?.maintenanceMarginRate)
        assertEquals(1, dispatcher.activeAssetDataRequestCount, "fetch still runs for tiers + spread")

        stream.stop()
        arca.close()
    }

    @Test
    fun maintenanceMarginRatePersistsAcrossPriceUpdates() = runBlocking {
        dispatcher.maintenanceMarginRate = "0.012"
        val arca = makeArca()
        val stream = launchWatch(arca, mmr = null)
        assertEquals("0.012", stream.activeAssetData.value?.maintenanceMarginRate)

        val update = async { withTimeoutOrNull(1_500) { stream.updates.first() } }
        delay(80)
        arca.ws.injectMessage("""{"type":"mids.updated","mids":{"hl:0:BTC":"80100"},"deliverySeq":1}""")

        val recomputed = update.await()
        assertNotNull(recomputed, "price tick must trigger a recompute")
        assertEquals("0.012", recomputed!!.maintenanceMarginRate, "recompute must reuse the dynamic MMR")
        assertEquals("0.012", stream.activeAssetData.value?.maintenanceMarginRate)

        stream.stop()
        arca.close()
    }

    @Test
    fun serverModeUsesServerActiveAssetDataAndIgnoresMidTicks() = runBlocking {
        dispatcher.exchangePricingMode = "server"
        dispatcher.maintenanceMarginRate = "0.01"
        val arca = makeArca()
        val stream = launchWatch(arca, mmr = null)

        // Server returns maxBuySize "0"; local derivation from equity/leverage/mark
        // would be non-zero, so "0" proves the value came from the server.
        assertEquals("0", stream.activeAssetData.value?.maxBuySize)
        assertEquals("80000", stream.activeAssetData.value?.markPx)

        val emitted = async { withTimeoutOrNull(400) { stream.updates.first() } }
        delay(50)
        arca.ws.injectMessage("""{"type":"mids.updated","mids":{"hl:0:BTC":"81000"},"deliverySeq":1}""")
        assertNull(emitted.await(), "server mode must ignore raw mid ticks")

        stream.stop()
        arca.close()
    }

    @Test
    fun quietServerPricedMaxSizeRefreshesWithoutMarketTicks() = runBlocking {
        dispatcher.exchangePricingMode = "server"
        dispatcher.refreshIntervalMs = 5000L
        val arca = makeArca()
        val stream = launchWatch(arca, mmr = null)
        assertEquals("0.01", stream.activeAssetData.value?.maintenanceMarginRate)
        dispatcher.maintenanceMarginRate = "0.025"
        withTimeout(7_000) { stream.activeAssetData.first { it?.maintenanceMarginRate == "0.025" } }
        assertEquals("0", stream.activeAssetData.value?.maxBuySize)
        assertEquals(3, dispatcher.activeAssetDataRequestCount)
        stream.stop()
        arca.close()
    }

    // MARK: - Late subscriber on a quiet market

    /**
     * Gobi's 2026-09-09 reproduction, with the outcome it should have had.
     *
     * An app-wide `watchPrices()` takes the 0→1 mids-subscription edge and so
     * is the only collector live when the server answers with its one
     * `mids.snapshot`. `watchMaxOrderSize` opens a second one, which used to
     * start from an empty map: for a market that does not tick between
     * subscribe and use, it returned "successfully" with no sizing and stayed
     * that way until that one market moved.
     */
    @Test
    fun lateSizingWatchInheritsRetainedSnapshotForQuietMarket() = runBlocking {
        val arca = makeArca()

        val appPrices = coroutineScope {
            val deferred = async { arca.watchPrices() }
            launch {
                delay(120)
                arca.ws.injectMessage(
                    """{"type":"mids.snapshot","mids":{"hl:0:BTC":"80000","hl:0:ADA":"0.5"}}""",
                )
            }
            withTimeout(3_000) { deferred.await() }
        }
        assertEquals("80000", appPrices.prices.value["hl:0:BTC"])

        // No further injection: the sizing watch has to resolve off the
        // retained map alone.
        val sizing = withTimeout(3_000) {
            arca.watchMaxOrderSize(
                MaxOrderSizeWatchOptions(
                    objectId = "obj_1",
                    market = "hl:0:BTC",
                    side = OrderSide.BUY,
                    leverage = 5,
                    feeScale = 1.0,
                ),
            )
        }

        assertNotNull(
            sizing.activeAssetData.value,
            "A late sizing watch must inherit the retained snapshot, not wait for its market to tick",
        )
        assertEquals(80000.0, sizing.activeAssetData.value?.markPx?.toDouble())
        assertNull(sizing.pendingReason.value)
        assertEquals(WatchStreamState.CONNECTED, sizing.state.value)

        sizing.stop()
        appPrices.stop()
        arca.close()
    }

    /**
     * The market is absent from the snapshot entirely (never ticked since the
     * socket opened), so the replay cannot supply its mark. The
     * `getActiveAssetData` response fetched at setup carries one, and it is
     * enough to return with sizing.
     */
    @Test
    fun activeAssetDataMarkSeedsSizingWhenMidsLackTheMarket() = runBlocking {
        val arca = makeArca()
        val sizing = launchWatch(arca, mmr = null, snapshot = """{"hl:0:ADA":"0.5"}""", awaitReady = false)

        assertNotNull(
            sizing.activeAssetData.value,
            "The setup active-asset-data read already carries a mark; sizing must use it",
        )
        assertEquals(80000.0, sizing.activeAssetData.value?.markPx?.toDouble())
        assertNull(sizing.pendingReason.value)

        // A live mid for the market supersedes the seed.
        arca.ws.injectMessage("""{"type":"mids.updated","mids":{"hl:0:BTC":"81000"},"deliverySeq":1}""")
        withTimeout(2_000) {
            sizing.activeAssetData.first { it?.markPx?.toDouble() == 81000.0 }
        }

        sizing.stop()
        arca.close()
    }

    /**
     * Neither the price map nor the active-asset-data read can price the
     * market. The stream must say so rather than return a silent null: `state`
     * stays `LOADING` and `pendingReason` names the missing input.
     */
    @Test
    fun pendingReasonReportsMissingMarkWhenNothingCanPriceTheMarket() = runBlocking {
        dispatcher.markPx = "0"
        val arca = makeArca()
        val sizing = launchWatch(arca, mmr = null, snapshot = """{"hl:0:ADA":"0.5"}""", awaitReady = false)

        assertNull(sizing.activeAssetData.value)
        assertEquals(
            MaxOrderSizePendingReason.AWAITING_MARK_PRICE,
            sizing.pendingReason.value,
            "An unpriced market is not a \$0 account, and the stream has to be able to say which",
        )
        assertEquals(WatchStreamState.LOADING, sizing.state.value)

        sizing.stop()
        arca.close()
    }

    // MARK: - Helpers

    /**
     * `watchMaxOrderSize` blocks on `watchPrices().ready()` until the first mid
     * arrives, so we run it and feed a snapshot concurrently once its mids
     * subscription is live (mirrors the Swift test's Task + inject pattern).
     */
    private suspend fun launchWatch(
        arca: Arca,
        mmr: String?,
        snapshot: String = """{"hl:0:BTC":"80000"}""",
        awaitReady: Boolean = true,
    ): MaxOrderSizeWatchStream =
        coroutineScope {
            val deferred = async {
                arca.watchMaxOrderSize(
                    MaxOrderSizeWatchOptions(
                        objectId = "obj_1",
                        market = "hl:0:BTC",
                        side = OrderSide.BUY,
                        leverage = 5,
                        feeScale = 1.0,
                        maintenanceMarginRate = mmr,
                    ),
                )
            }
            launch {
                delay(150)
                arca.ws.injectMessage("""{"type":"mids.snapshot","mids":$snapshot}""")
            }
            val stream = withTimeout(3_000) { deferred.await() }
            // A snapshot that cannot price the selected market never reaches
            // CONNECTED, so those cases must not await readiness.
            if (awaitReady) stream.ready()
            stream
        }

    private fun makeArca(): Arca = Arca(token = fakeJwt(), baseUrl = server.url("/").toString().trimEnd('/'))

    private fun fakeJwt(): String {
        val enc = Base64.getUrlEncoder().withoutPadding()
        val header = enc.encodeToString("""{"alg":"HS256","typ":"JWT"}""".toByteArray())
        val payload = enc.encodeToString("""{"realmId":"rlm_test","sub":"usr_test"}""".toByteArray())
        return "$header.$payload.fakesig"
    }
}

private class MaxOrderSizeDispatcher : Dispatcher() {
    @Volatile var maintenanceMarginRate = "0.01"
    @Volatile var exchangePricingMode: String? = null
    @Volatile var refreshIntervalMs: Long? = null

    /**
     * `markPx` on the active-asset-data response. `"0"` models a market the
     * venue cannot price at all, which is the only case that should leave the
     * sizing stream without a value.
     */
    @Volatile var markPx = "80000"
    private val aadCount = AtomicInteger(0)
    val activeAssetDataRequestCount: Int get() = aadCount.get()

    override fun dispatch(request: RecordedRequest): MockResponse {
        val path = (request.path ?: "").substringBefore("?")
        return when {
            path.endsWith("/exchange/state") -> json(stateBody())
            path.endsWith("/exchange/active-asset-data") -> {
                aadCount.incrementAndGet()
                json(activeAssetDataBody())
            }
            path.endsWith("/objects/obj_1") -> json(OBJECT_DETAIL)
            else -> MockResponse().setResponseCode(404)
        }
    }

    private fun stateBody(): String {
        val pm = (exchangePricingMode?.let { "\"pricingMode\":\"$it\"," } ?: "") +
            (refreshIntervalMs?.let { "\"stateRefreshIntervalMs\":$it," } ?: "")
        return """
            {"success":true,"data":{
              $pm"account":{"id":"act_1","realmId":"rlm_test","name":"main","createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z"},
              "marginSummary":{"equity":"10000","initialMarginUsed":"0","maintenanceMarginRequired":"0","availableToWithdraw":"10000","totalNtlPos":"0","totalUnrealizedPnl":"0"},
              "positions":[],"openOrders":[],
              "feeRates":{"taker":"0.00035","maker":"0.0001","platformFee":"0.0001"},
              "pendingIntents":[]
            }}
        """.trimIndent()
    }

    private fun activeAssetDataBody(): String = """
        {"success":true,"data":{
          "market":"hl:0:BTC",
          "leverage":{"type":"cross","value":5},
          "maxBuySize":"0","maxSellSize":"0","maxBuyUsd":"0","maxSellUsd":"0",
          "availableToTrade":"10000","markPx":"$markPx","feeRate":"0.00045",
          "maintenanceMarginRate":"$maintenanceMarginRate"
        }}
    """.trimIndent()

    private fun json(body: String): MockResponse =
        MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json").setBody(body)

    private companion object {
        val OBJECT_DETAIL = """
            {"success":true,"data":{"object":{
              "id":"obj_1","realmId":"rlm_test","path":"/exchanges/main","type":"exchange",
              "denomination":"USD","status":"active","systemOwned":false,
              "createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z"
            }}}
        """.trimIndent()
    }
}
