package network.arca.sdk

import kotlinx.coroutines.runBlocking
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

class MarketMetaCacheTest {

    private lateinit var server: MockWebServer
    private lateinit var dispatcher: MetaDispatcher

    @BeforeEach
    fun setUp() {
        dispatcher = MetaDispatcher()
        server = MockWebServer()
        server.dispatcher = dispatcher
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    private fun makeArca(): Arca =
        Arca(token = fakeJwt(), baseUrl = server.url("/").toString().trimEnd('/'))

    private fun fakeJwt(): String {
        val enc = Base64.getUrlEncoder().withoutPadding()
        val header = enc.encodeToString("""{"alg":"HS256","typ":"JWT"}""".toByteArray())
        val payload = enc.encodeToString("""{"realmId":"rlm_test","sub":"usr_test"}""".toByteArray())
        return "$header.$payload.fakesig"
    }

    // MARK: - market(id) exact-id lookup

    @Test
    fun marketReturnsCachedMetadata() = runBlocking {
        val arca = makeArca()
        val btc = arca.market("hl:0:BTC")
        assertNotNull(btc)
        assertEquals("BTC", btc?.symbol)
        assertEquals("BTC", btc?.venueSymbol)
        assertEquals("hl", btc?.exchange)
        assertEquals(50, btc?.maxLeverage)
        assertEquals("https://example.com/btc.png", btc?.logoUrl)
        assertEquals(1, btc?.logoSources?.size)
        assertEquals(128, btc?.logoSources?.first()?.width)
        assertEquals("crypto", btc?.assetType)
        assertEquals("Crypto", btc?.categoryLabel)
        assertEquals(true, btc?.mapped)
        assertEquals(true, btc?.hasLogo)
        assertEquals("curated", btc?.descriptionStatus)
        assertEquals(1, dispatcher.metaRequestCount.get())
    }

    @Test
    fun marketReturnsNilForUnknownCoin() = runBlocking {
        val arca = makeArca()
        val unknown = arca.market("hl:0:DOESNOTEXIST")
        assertNull(unknown)
        assertEquals(1, dispatcher.metaRequestCount.get())
    }

    @Test
    fun subsequentCallsUseCacheWithoutRefetch() = runBlocking {
        val arca = makeArca()
        arca.market("hl:0:BTC")
        assertEquals(1, dispatcher.metaRequestCount.get())

        val eth = arca.market("hl:0:ETH")
        assertNotNull(eth)
        assertEquals("ETH", eth?.symbol)
        assertEquals(1, dispatcher.metaRequestCount.get())
    }

    @Test
    fun preloadPopulatesCache() = runBlocking {
        val arca = makeArca()
        arca.preloadMarketMeta()
        assertEquals(1, dispatcher.metaRequestCount.get())

        val btc = arca.market("hl:0:BTC")
        assertNotNull(btc)
        assertEquals(1, dispatcher.metaRequestCount.get())
    }

    @Test
    fun refreshReplacesCache() = runBlocking {
        val arca = makeArca()
        assertNotNull(arca.market("hl:0:BTC"))
        assertEquals(1, dispatcher.metaRequestCount.get())

        arca.refreshMarketMeta()
        assertEquals(2, dispatcher.metaRequestCount.get())

        assertNotNull(arca.market("hl:0:BTC"))
        assertEquals(2, dispatcher.metaRequestCount.get())
    }

    @Test
    fun hip3MarketLookup() = runBlocking {
        val arca = makeArca()
        val tsla = arca.market("hl:1:TSLA")
        assertNotNull(tsla)
        assertEquals("TSLA", tsla?.symbol)
        assertEquals("xyz:TSLA", tsla?.venueSymbol)
        assertEquals("Tesla", tsla?.displayName)
        assertEquals("equity", tsla?.assetType)
        assertEquals("Equity", tsla?.categoryLabel)
        assertEquals(true, tsla?.hasDisplayName)
        assertEquals("curated", tsla?.descriptionStatus)
        assertEquals(true, tsla?.isHip3)
        assertEquals(3.0, tsla?.feeScale)
    }

    @Test
    fun retriesAfterFailedFetch() = runBlocking {
        dispatcher.failNextN.set(1)
        val arca = makeArca()

        val firstThrown = runCatching { arca.market("hl:0:BTC") }.exceptionOrNull()
        assertNotNull(firstThrown)
        assertEquals(1, dispatcher.metaRequestCount.get())

        val btc = arca.market("hl:0:BTC")
        assertNotNull(btc)
        assertEquals("BTC", btc?.symbol)
        assertEquals(2, dispatcher.metaRequestCount.get())
    }

    // MARK: - resolveMarkets

    @Test
    fun resolveMarketsReturnsAllForSymbol() = runBlocking {
        val arca = makeArca()
        val markets = arca.resolveMarkets("BTC")
        assertEquals(2, markets.size)
        assertEquals(setOf("hl:0:BTC", "hl:1:BTC"), markets.map { it.name }.toSet())
    }

    @Test
    fun resolveMarketsSingleMatch() = runBlocking {
        val arca = makeArca()
        val markets = arca.resolveMarkets("ETH")
        assertEquals(1, markets.size)
        assertEquals("hl:0:ETH", markets.first().name)
    }

    @Test
    fun resolveMarketsNoMatchReturnsEmpty() = runBlocking {
        val arca = makeArca()
        assertEquals(0, arca.resolveMarkets("NOPE").size)
    }

    @Test
    fun resolveMarketsFilterByDex() = runBlocking {
        val arca = makeArca()
        val markets = arca.resolveMarkets("BTC", dex = "xyz")
        assertEquals(1, markets.size)
        assertEquals("hl:1:BTC", markets.first().name)
    }

    @Test
    fun resolveMarketsFilterByExchange() = runBlocking {
        val arca = makeArca()
        assertEquals(2, arca.resolveMarkets("BTC", exchange = "hl").size)
        assertEquals(0, arca.resolveMarkets("BTC", exchange = "pm").size)
    }

    @Test
    fun resolveMarketsCaseSensitive() = runBlocking {
        val arca = makeArca()
        assertEquals(0, arca.resolveMarkets("btc").size)
    }

    // MARK: - resolveMarketOrThrow

    @Test
    fun resolveMarketOrThrowSingle() = runBlocking {
        val arca = makeArca()
        assertEquals("hl:0:ETH", arca.resolveMarketOrThrow("ETH").name)
    }

    @Test
    fun resolveMarketOrThrowZeroThrows() = runBlocking {
        val arca = makeArca()
        val thrown = runCatching { arca.resolveMarketOrThrow("NOPE") }.exceptionOrNull()
        val v = thrown as? ArcaException.Validation
        assertNotNull(v)
        assertTrue(v?.message?.contains("No market found") == true, "got: ${v?.message}")
    }

    @Test
    fun resolveMarketOrThrowAmbiguousThrows() = runBlocking {
        val arca = makeArca()
        val thrown = runCatching { arca.resolveMarketOrThrow("BTC") }.exceptionOrNull()
        val v = thrown as? ArcaException.Validation
        assertNotNull(v)
        assertTrue(v?.message?.contains("ambiguous") == true, "got: ${v?.message}")
    }

    @Test
    fun resolveMarketOrThrowNarrowedByDex() = runBlocking {
        val arca = makeArca()
        assertEquals("hl:1:BTC", arca.resolveMarketOrThrow("BTC", dex = "xyz").name)
    }
}

private class MetaDispatcher : Dispatcher() {
    val metaRequestCount = AtomicInteger(0)
    val failNextN = AtomicInteger(0)

    override fun dispatch(request: RecordedRequest): MockResponse {
        val path = request.path ?: ""
        if (path.contains("/exchange/market/meta")) {
            metaRequestCount.incrementAndGet()
            if (failNextN.get() > 0) {
                failNextN.decrementAndGet()
                return MockResponse()
                    .setResponseCode(500)
                    .setHeader("Content-Type", "application/json")
                    .setBody("""{"success":false,"error":{"code":"INTERNAL","message":"boom"}}""")
            }
            return MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody(UNIVERSE_JSON)
        }
        return MockResponse().setResponseCode(404)
    }
}

private val UNIVERSE_JSON = """
{
  "success": true,
  "data": {
    "universe": [
      {
        "name": "hl:0:BTC", "dex": null, "symbol": "BTC", "venueSymbol": "BTC", "displayName": null,
        "logoUrl": "https://example.com/btc.png",
        "logoSources": [{"url": "https://example.com/btc-128.webp", "format": "webp", "width": 128}],
        "exchange": "hl", "assetType": "crypto", "categoryLabel": "Crypto", "mapped": true,
        "hasDisplayName": false, "hasLogo": true, "descriptionStatus": "curated", "isHip3": false,
        "deployerDisplayName": null, "index": 0, "szDecimals": 5, "maxLeverage": 50, "onlyIsolated": false, "feeScale": 1.0
      },
      {
        "name": "hl:0:ETH", "dex": null, "symbol": "ETH", "venueSymbol": "ETH", "displayName": null,
        "logoUrl": "https://example.com/eth.png",
        "exchange": "hl", "assetType": "crypto", "categoryLabel": "Crypto", "mapped": true,
        "hasDisplayName": false, "hasLogo": true, "descriptionStatus": "curated", "isHip3": false,
        "deployerDisplayName": null, "index": 1, "szDecimals": 4, "maxLeverage": 50, "onlyIsolated": false, "feeScale": 1.0
      },
      {
        "name": "hl:1:TSLA", "dex": "xyz", "symbol": "TSLA", "venueSymbol": "xyz:TSLA", "displayName": "Tesla",
        "logoUrl": "https://example.com/tsla.png",
        "exchange": "hl", "assetType": "equity", "categoryLabel": "Equity", "mapped": true,
        "hasDisplayName": true, "hasLogo": true, "descriptionStatus": "curated", "isHip3": true,
        "deployerDisplayName": "xyz", "index": 2, "szDecimals": 2, "maxLeverage": 5, "onlyIsolated": false, "feeScale": 3.0
      },
      {
        "name": "hl:1:BTC", "dex": "xyz", "symbol": "BTC", "venueSymbol": "xyz:BTC", "displayName": null,
        "logoUrl": "https://example.com/btc.png",
        "exchange": "hl", "assetType": "crypto", "categoryLabel": "Crypto", "mapped": true,
        "hasDisplayName": false, "hasLogo": true, "descriptionStatus": "curated", "isHip3": true,
        "deployerDisplayName": "xyz", "index": 3, "szDecimals": 5, "maxLeverage": 20, "onlyIsolated": false, "feeScale": 2.0
      }
    ]
  }
}
""".trimIndent()
