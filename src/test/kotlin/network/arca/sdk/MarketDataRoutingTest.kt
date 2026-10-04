package network.arca.sdk

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.serialization.json.*
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.*
import okhttp3.OkHttpClient
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.Collections

class MarketDataRoutingTest {
    private fun market(name: String = "hl:0:BTC", coin: String = "BTC") = arcaJson.decodeFromString<Market>(
        """{"name":"$name","venueSymbol":"$coin","symbol":"BTC","exchange":"hl","index":0,"szDecimals":5,"maxLeverage":50,"onlyIsolated":false}""")
    private fun router(markets: List<Market> = listOf(market())) = MarketDataRouter().apply {
        configure(MarketDataPreference.HYPERLIQUID, markets); register("watch"); update("watch", markets.map { it.name }.toSet(), 0)
    }
    @Test fun staleArcaFramesCannotOverwriteSelectedPriceAndFailureUsesNextLiveFrame() {
        val r = router(); val prices = mapOf("hl:0:BTC" to "90", "gllt:3" to "12")
        assertEquals(prices, r.arcaPrices(prices))
        assertEquals("100", r.direct(PublicMarketUpdate.Quote("hl:0:BTC", "100", 1000), 1000)?.mids?.get("hl:0:BTC"))
        assertEquals(mapOf("gllt:3" to "12"), r.arcaPrices(prices))
        assertNull(r.direct(PublicMarketUpdate.Quote("hl:0:BTC", "99", 999), 1000))
        assertNull(r.direct(PublicMarketUpdate.Unavailable("offline"), 1000))
        assertEquals(prices, r.arcaPrices(prices))
        r.configure(MarketDataPreference.ARCA, emptyList())
        assertTrue(r.subscriptions.isEmpty())
        assertNull(r.direct(PublicMarketUpdate.Quote("hl:0:BTC", "101", 1001), 1001))
    }
    @Test fun lateMetadataConfigurationCannotUndoRollback() {
        val manager = WebSocketManager("http://localhost:1", "token", "realm", OkHttpClient())
        manager.publicSourceFactory = { _, _ -> error("rolled-back configuration must not open a socket") }
        val owner = manager.registerPriceMarkets(); manager.updatePriceMarkets(owner, setOf("hl:0:BTC"))
        val old = manager.beginMarketDataConfiguration()
        val current = manager.beginMarketDataConfiguration()
        manager.configureMarketData(current, MarketDataPreference.ARCA, HyperliquidNetwork.MAINNET, emptyList())
        manager.configureMarketData(old, MarketDataPreference.HYPERLIQUID, HyperliquidNetwork.MAINNET, listOf(market()))
        assertEquals(MarketDataPreference.ARCA, manager.marketDataSourceStatus.preference)
        manager.disconnect()
    }
    @Test fun exactMetadataSeparatesSameTickerAndRejectsAmbiguousMapping() {
        val markets = listOf(market(), market("hl:1:BTC", "xyz:BTC"), market("hl:2:BAD", "same"), market("hl:3:BAD", "same"), market("hl:4:DUP", "a"), market("hl:4:DUP", "b"))
        val r = router(markets)
        assertEquals(setOf(PublicMarketSubscription("hl:0:BTC", "BTC"), PublicMarketSubscription("hl:1:BTC", "xyz:BTC")), r.subscriptions)
    }
    @Test fun releasedWatchCannotReopenFromLateUpdateAndSubscriptionsAreBounded() {
        val r = router((0..99).map { market("hl:0:C$it", "C$it") })
        assertEquals(64, r.subscriptions.size)
        r.update("watch", setOf("hl:0:C1"), 2); r.update("watch", setOf("hl:0:C2"), 1)
        assertEquals("hl:0:C1", r.subscriptions.single().market)
        r.release("watch"); r.update("watch", setOf("hl:0:C1"), 3)
        assertTrue(r.subscriptions.isEmpty())
    }
    @Test fun oneCandleWatchStoppingCannotRemoveAnotherAndClosedBarWins() {
        val r = router(); val iv = CandleInterval.ONE_MINUTE
        repeat(2) { r.acquireCandles(listOf("hl:0:BTC"), listOf(iv, CandleInterval.FIFTEEN_SECONDS)) }
        r.releaseCandles(listOf("hl:0:BTC"), listOf(iv, CandleInterval.FIFTEEN_SECONDS))
        assertEquals(2, r.subscriptions.size)
        val c = Candle(t = 60_000, o = "100", h = "101", l = "99", c = "101", v = "2", n = 2)
        assertNotNull(r.direct(PublicMarketUpdate.Bar("hl:0:BTC", iv, c), 61_000))
        assertFalse(r.arcaCandle(RealmEvent(type = "candle.updated", market = "hl:0:BTC", interval = "1m", candle = c)))
        assertTrue(r.arcaCandle(RealmEvent(type = "candle.closed", market = "hl:0:BTC", interval = "1m", candle = c)))
        assertNull(r.direct(PublicMarketUpdate.Bar("hl:0:BTC", iv, c.copy(n = 3)), 61_000))
        r.releaseCandles(listOf("hl:0:BTC"), listOf(iv, CandleInterval.FIFTEEN_SECONDS))
        assertEquals(1, r.subscriptions.size)
    }
    @Test fun publicBurstPublishesImmediateThenLatestBatchAndFailureCancelsPending() = runBlocking {
        val manager = WebSocketManager("http://localhost:1", "token", "realm", OkHttpClient())
        lateinit var receive: (Long, PublicMarketUpdate) -> Unit
        manager.publicSourceFactory = { _, callback -> receive = callback; object : PublicMarketSource {
            override fun subscribe(subscriptions: Set<PublicMarketSubscription>) {}
            override fun close() {}
        } }
        val owner = manager.registerPriceMarkets(); manager.updatePriceMarkets(owner, setOf("hl:0:BTC"))
        manager.configureMarketData(manager.beginMarketDataConfiguration(), MarketDataPreference.HYPERLIQUID, HyperliquidNetwork.MAINNET, listOf(market()))
        val values = Collections.synchronizedList(mutableListOf<String>())
        val collector = launch(start = CoroutineStart.UNDISPATCHED) { manager.midsEvents().collect { values += it.getValue("hl:0:BTC") } }
        delay(30)
        val now = System.currentTimeMillis()
        receive(1, PublicMarketUpdate.Quote("hl:0:BTC", "100", now))
        withTimeout(1000) { while (values.isEmpty()) delay(1) }
        assertEquals("100", values.single())
        val start = System.nanoTime()
        for (i in 1..500) receive(1, PublicMarketUpdate.Quote("hl:0:BTC", "${100 + i}", now + i))
        val allowed = 2 + (System.nanoTime() - start) / 100_000_000
        withTimeout(1000) { while (values.lastOrNull() != "600") delay(1) }
        assertTrue(values.size <= allowed, "burst must batch before valuation fan-out")
        val count = values.size
        receive(1, PublicMarketUpdate.Quote("hl:0:BTC", "999", now + 501))
        receive(2, PublicMarketUpdate.Unavailable("offline"))
        delay(150)
        assertEquals(count, values.size, "failed source must not flush queued prices")
        collector.cancel(); manager.disconnect()
    }
    @Test fun bboDecoderPreservesHalfDollarAndNeverCopiesVenueSymbolIntoCandleProvenance() {
        val subs = setOf(PublicMarketSubscription("hl:1:BTC", "xyz:BTC"), PublicMarketSubscription("hl:1:BTC", "xyz:BTC", CandleInterval.ONE_MINUTE))
        fun decode(json: String) = decodePublicMarketUpdate(arcaJson.parseToJsonElement(json).jsonObject, subs)
        val bbo = """{"channel":"bbo","data":{"coin":"xyz:BTC","time":1000,"bbo":[{"px":"84970"},{"px":"84971"}]}}"""
        assertEquals("84970.5", (decode(bbo) as PublicMarketUpdate.Quote).price)
        assertNull(decode(bbo.replace("84970", "1e99999999")))
        assertNull(decode(bbo.replace("84970", "90000")))
        assertNull(decode(bbo.replace("xyz:BTC", "BTC")))
        val c = decode("""{"channel":"candle","data":{"s":"xyz:BTC","i":"1m","t":60000,"o":"100","h":"101","l":"99","c":"101","v":"2","n":2}}""") as PublicMarketUpdate.Bar
        assertNull(c.candle.s)
    }
    @Test fun selectedSourceFeedsSharedValuationBusAndOldConnectionCannotReturnAfterFailure() = runBlocking {
        val manager = WebSocketManager("http://localhost:1", "private-token", "realm", OkHttpClient())
        lateinit var receive: (Long, PublicMarketUpdate) -> Unit
        var closed = false
        manager.publicSourceFactory = { _, callback -> receive = callback; object : PublicMarketSource {
            override fun subscribe(subscriptions: Set<PublicMarketSubscription>) {}
            override fun close() { closed = true }
        } }
        val owner = manager.registerPriceMarkets()
        manager.updatePriceMarkets(owner, setOf("hl:0:BTC"))
        manager.configureMarketData(manager.beginMarketDataConfiguration(), MarketDataPreference.HYPERLIQUID, HyperliquidNetwork.MAINNET, listOf(market()))
        val values = Collections.synchronizedList(mutableListOf<String>())
        val collector = launch(start = CoroutineStart.UNDISPATCHED) { manager.midsEvents().collect { it["hl:0:BTC"]?.let(values::add) } }
        delay(60)
        fun arca(price: String) = manager.injectMessage("""{"type":"mids.updated","mids":{"hl:0:BTC":"$price"}}""")
        val now = System.currentTimeMillis()
        arca("100"); receive(1, PublicMarketUpdate.Quote("hl:0:BTC", "101", now)); arca("102")
        receive(1, PublicMarketUpdate.Quote("hl:0:BTC", "101", now)) // quantity-only tick
        receive(2, PublicMarketUpdate.Unavailable("offline")); receive(1, PublicMarketUpdate.Quote("hl:0:BTC", "999", now))
        arca("103"); receive(3, PublicMarketUpdate.Quote("hl:0:BTC", "105", now + 1)); arca("104"); manager.flushPublicPrices()
        manager.beginMarketDataConfiguration(); receive(3, PublicMarketUpdate.Quote("hl:0:BTC", "999", now + 2)); arca("107")
        withTimeout(2000) { while (values.size < 5) delay(5) }
        assertEquals(listOf("100", "101", "103", "105", "107"), values.toList())
        val diagnostics = manager.marketDataDiagnostics
        assertEquals(3L, diagnostics.arcaPriceValues); assertEquals(2L, diagnostics.hyperliquidPriceValues)
        assertEquals(1L, diagnostics.hyperliquidFailures); assertEquals(1L, diagnostics.hyperliquidRecoveries)
        assertEquals(1, diagnostics.arcaServingMarkets); assertEquals(0, diagnostics.hyperliquidServingMarkets)
        assertTrue(closed)
        assertEquals("107", withTimeout(2000) { manager.midsEvents().first() }["hl:0:BTC"])
        val base = arcaJson.decodeFromString<ExchangeState>("""{"account":{"id":"a1","realmId":"r1","name":"main","createdAt":"2026-09-06","updatedAt":"2026-09-06"},"marginSummary":{"equity":"1000","initialMarginUsed":"50","maintenanceMarginRequired":"0","availableToWithdraw":"950","totalNtlPos":"1000","totalUnrealizedPnl":"0","totalRawUsd":"1000"},"positions":[{"id":"p1","market":"hl:0:BTC","side":"long","size":"10","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"}],"openOrders":[]}""")
        val selected = withTimeout(2000) { manager.midsEvents().first() }
        val marked = base.revalued(selected)
        assertEquals(1070.0, marked.marginSummary.equity.toDouble())
        assertEquals(70.0, marked.positions.single().unrealizedPnl!!.toDouble())
        assertNotEquals(deriveActiveAssetData(base, "hl:0:BTC", 100.0, 20, OrderSide.BUY)?.maxBuySize,
            deriveActiveAssetData(marked, "hl:0:BTC", 107.0, 20, OrderSide.BUY)?.maxBuySize)
        assertEquals("1000", base.copy(pricingMode = PricingMode.SERVER).revalued(selected).marginSummary.equity)
        collector.cancel(); manager.disconnect()
    }
}
