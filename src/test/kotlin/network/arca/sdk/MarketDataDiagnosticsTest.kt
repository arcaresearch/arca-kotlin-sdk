package network.arca.sdk

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import okhttp3.OkHttpClient

class MarketDataDiagnosticsTest {
    @Test fun preferredSourceDoesNotMislabelFallbackAndQuietPriceIsNotFailure() {
        val router = MarketDataRouter(); val r = MarketDataDiagnosticsRecorder()
        router.register("owner"); router.update("owner", setOf("hl:0:BTC"), 1)
        router.configure(MarketDataPreference.HYPERLIQUID, emptyList())
        r.prices(mapOf("hl:0:BTC" to "100", "hl:0:ETH" to "10"), false, router.priceInterestMarkets)
        var s = r.snapshot(router)
        assertEquals(MarketDataPreference.HYPERLIQUID, s.preference); assertEquals(1, s.arcaServingMarkets)
        assertEquals(1L, s.arcaPriceValues); assertEquals(0L, s.hyperliquidFailures)
        r.prices(mapOf("hl:0:BTC" to "101"), true, router.priceInterestMarkets)
        r.failure(); r.failure()
        r.prices(mapOf("hl:0:BTC" to "101"), false, router.priceInterestMarkets)
        r.recovered(); r.recovered()
        s = r.snapshot(router)
        assertEquals(1, s.arcaServingMarkets); assertEquals(0, s.hyperliquidServingMarkets)
        assertEquals(2L, s.hyperliquidFailures); assertEquals(1L, s.hyperliquidRecoveries)
        assertNotNull(s.firstPriceMs)
        router.release("owner"); r.retain(router.priceInterestMarkets)
        assertEquals(0, r.snapshot(router).arcaServingMarkets); assertEquals(2L, r.snapshot(router).arcaPriceValues)
    }
    @Test fun cumulativeCountersSurviveDroppedSnapshotsAndSuspendDoesNotCountRecovery() {
        val r = MarketDataDiagnosticsRecorder(); val router = MarketDataRouter()
        repeat(100) { r.bytes(100, true); r.candle(true) }
        r.bytes(400, false); r.connected(true); r.connected(false); r.connected(false)
        r.failure(); r.suspend(); r.recovered()
        val s = r.snapshot(router)
        assertEquals(10_000L, s.hyperliquidPayloadBytes); assertEquals(100L, s.hyperliquidCandleFrames)
        assertEquals(400L, s.arcaPayloadBytes); assertEquals(1L, s.arcaDisconnects); assertEquals(0L, s.hyperliquidRecoveries)
    }
    @Test fun watchingDiagnosticsDoesNotSubscribeOrOpenASocket() {
        val manager = WebSocketManager("http://localhost:1", "test", "test", OkHttpClient())
        manager.publicSourceFactory = { _, _ -> error("Observer must not open a socket") }
        assertEquals(0, manager.watchMarketDataDiagnostics().value.requestedPriceMarkets)
        val owner = manager.registerPriceMarkets(); manager.updatePriceMarkets(owner, setOf("hl:0:BTC"))
        manager.injectMessage("""{"type":"mids.updated","mids":{"hl:0:BTC":"100"}}""")
        val s = manager.marketDataDiagnostics
        assertEquals(1L, s.arcaPriceValues); assertTrue(s.arcaPayloadBytes > 0); assertEquals(0, s.directSubscriptions)
        manager.disconnect()
    }
}
