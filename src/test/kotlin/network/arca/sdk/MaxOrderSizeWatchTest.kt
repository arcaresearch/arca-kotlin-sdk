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

    // MARK: - Helpers

    /**
     * `watchMaxOrderSize` blocks on `watchPrices().ready()` until the first mid
     * arrives, so we run it and feed a snapshot concurrently once its mids
     * subscription is live (mirrors the Swift test's Task + inject pattern).
     */
    private suspend fun launchWatch(arca: Arca, mmr: String?): MaxOrderSizeWatchStream =
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
                arca.ws.injectMessage("""{"type":"mids.snapshot","mids":{"hl:0:BTC":"80000"}}""")
            }
            val stream = withTimeout(3_000) { deferred.await() }
            stream.ready()
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
        val pm = exchangePricingMode?.let { "\"pricingMode\":\"$it\"," } ?: ""
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
          "availableToTrade":"10000","markPx":"80000","feeRate":"0.00045",
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
