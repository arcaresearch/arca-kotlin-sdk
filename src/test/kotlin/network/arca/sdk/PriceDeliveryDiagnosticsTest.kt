package network.arca.sdk

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.collect
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.Market
import okhttp3.*
import okhttp3.mockwebserver.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class PriceDeliveryDiagnosticsTest {
    private val btc = "hl:0:BTC"
    private val eth = "hl:0:ETH"
    @Test fun optionalMainnetReceiptAndRecoveryTimingProbe() = runBlocking {
        org.junit.jupiter.api.Assumptions.assumeTrue(System.getenv("ARCA_PUBLIC_TIMING_PROBE") == "1")
        val manager = WebSocketManager("http://localhost:1", "unused", "unused", OkHttpClient())
        val snapshots = Channel<Map<String, PriceDeliveryDiagnostic>>(Channel.CONFLATED)
        val job = launch(start = CoroutineStart.UNDISPATCHED) { manager.watchPriceDeliveryDiagnostics(setOf(btc)).collect { snapshots.send(it) } }
        try {
            val owner = manager.registerPriceMarkets(); manager.updatePriceMarkets(owner, setOf(btc))
            val market = arcaJson.decodeFromString<Market>("""{"name":"hl:0:BTC","venueSymbol":"BTC","symbol":"BTC","exchange":"hl","index":0,"szDecimals":5,"maxLeverage":50,"onlyIsolated":false}""")
            manager.configureMarketData(manager.beginMarketDataConfiguration(), MarketDataPreference.HYPERLIQUID, HyperliquidNetwork.MAINNET, listOf(market))
            suspend fun nextRecord(): PriceDeliveryDiagnostic {
                while (true) snapshots.receive()[btc]?.let { return it }
            }
            fun record(value: PriceDeliveryDiagnostic) {
                assertTrue(value.publishedAtNanos >= value.receivedAtNanos); assertNotNull(value.venueTimeMs)
                println("PUBLIC_TIMING_PROBE kotlin HL-mainnet revision=${value.revision} receipt_to_publish_ms=${(value.publishedAtNanos - value.receivedAtNanos) / 1e6}")
            }
            record(withTimeout(20_000) { nextRecord() })
            // Quiet prices are valid. Require one initial quote, then sample a bounded window.
            withTimeoutOrNull(10_000) { repeat(4) { record(nextRecord()) } }
            assertEquals(0L, manager.marketDataDiagnostics.hyperliquidFailures)
            val timing = manager.marketDataDiagnostics.hyperliquidConnection
            assertNotNull(timing.openedAtNanos); assertNotNull(timing.firstPriceAtNanos)
            println("PUBLIC_TIMING_CONNECTION kotlin HL-mainnet handshake_ms=${(timing.openedAtNanos!! - timing.attemptAtNanos!!) / 1e6} open_to_price_ms=${(timing.firstPriceAtNanos!! - timing.openedAtNanos!!) / 1e6}")
        } finally { job.cancelAndJoin(); manager.disconnect() }
    }
    @Test fun sourceReportsActualRetryDelayAndReconnectHandshake() = runBlocking {
        val server = MockWebServer()
        val firstSocket = Channel<WebSocket>(1)
        repeat(2) { index -> server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) { if (index == 0) firstSocket.trySend(webSocket) }
            override fun onMessage(webSocket: WebSocket, text: String) {
                if (text.contains("subscribe")) webSocket.send("""{"channel":"bbo","data":{"coin":"BTC","time":${System.currentTimeMillis()},"bbo":[{"px":"${100 + index}"},{"px":"${100 + index}"}]}}""")
            }
        })) }
        server.start()
        val manager = WebSocketManager("http://localhost:1", "unused", "unused", OkHttpClient())
        manager.publicSourceFactory = { _, receive -> HyperliquidMarketSource(server.url("/ws").toString(), receive) }
        val snapshots = Channel<Map<String, PriceDeliveryDiagnostic>>(Channel.CONFLATED)
        val job = launch(start = CoroutineStart.UNDISPATCHED) { manager.watchPriceDeliveryDiagnostics(setOf(btc)).collect { snapshots.send(it) } }
        try {
            val owner = manager.registerPriceMarkets(); manager.updatePriceMarkets(owner, setOf(btc))
            val market = arcaJson.decodeFromString<Market>("""{"name":"hl:0:BTC","venueSymbol":"BTC","symbol":"BTC","exchange":"hl","index":0,"szDecimals":5,"maxLeverage":50,"onlyIsolated":false}""")
            manager.configureMarketData(manager.beginMarketDataConfiguration(), MarketDataPreference.HYPERLIQUID, HyperliquidNetwork.MAINNET, listOf(market))
            suspend fun observed(price: String) = withTimeout(6000) {
                while (snapshots.receive()[btc]?.price != price) { /* await the next stream event */ }
            }
            observed("100")
            withTimeout(2000) { firstSocket.receive() }.close(1001, "test disconnect")
            observed("101")
            val timing = manager.marketDataDiagnostics.hyperliquidConnection
            assertTrue(timing.lastRetryDelayMs!! in 2000..2499)
            val actualWaitMs = (timing.attemptAtNanos!! - timing.lastRetryScheduledAtNanos!!) / 1e6
            assertTrue(actualWaitMs >= timing.lastRetryDelayMs!! - 1)
            assertTrue(timing.openedAtNanos!! >= timing.attemptAtNanos!!)
            assertTrue(timing.firstPriceAtNanos!! >= timing.openedAtNanos!!)
            println("LOCAL_RECOVERY_PROBE scheduled_ms=${timing.lastRetryDelayMs} actual_wait_ms=$actualWaitMs handshake_ms=${(timing.openedAtNanos!! - timing.attemptAtNanos!!) / 1e6} open_to_price_ms=${(timing.firstPriceAtNanos!! - timing.openedAtNanos!!) / 1e6}")
        } finally { job.cancelAndJoin(); manager.disconnect(); server.shutdown() }
    }
    @Test fun exactValuesKeepReceiptsAcrossQuietSnapshotsAndABAChanges() {
        val r = PriceDeliveryRecorder(setOf(btc, eth))
        r.stage(mapOf(btc to "100", eth to "10", "hl:0:SOL" to "1"), MarketDataPreference.ARCA, 10, null)
        val first = r.publish(mapOf(btc to "100", eth to "10"), 20)!!
        assertEquals(2, first.size); assertEquals(10L, first[btc]!!.receivedAtNanos)
        assertNull(r.publish(mapOf(btc to "100", eth to "10"), 30))
        r.stage(mapOf(btc to "101"), MarketDataPreference.HYPERLIQUID, 40, 123)
        val second = r.publish(mapOf(btc to "101", eth to "10"), 50)!!
        assertEquals(first[eth], second[eth]); assertEquals(123L, second[btc]!!.venueTimeMs)
        r.stage(mapOf(btc to "100"), MarketDataPreference.HYPERLIQUID, 60, null)
        val third = r.publish(mapOf(btc to "100", eth to "10"), 70)!!
        assertTrue(third[btc]!!.revision > first[btc]!!.revision)
        assertEquals(60L, third[btc]!!.receivedAtNanos); assertNull(third[btc]!!.venueTimeMs)
        assertEquals("100", first[btc]!!.price) // old immutable snapshots stay paired
    }
    @Test fun unrelatedPublicationCannotFlushDirectPendingAndFailureDiscardsIt() {
        val r = PriceDeliveryRecorder(setOf(btc, eth))
        r.stage(mapOf(btc to "100"), MarketDataPreference.HYPERLIQUID, 10, null)
        assertNull(r.publish(mapOf(btc to "100", eth to "10"), 20, setOf(eth)))
        r.clearDirectPending()
        assertNull(r.publish(mapOf(btc to "100"), 30))
        r.stage(mapOf(btc to "101", eth to "11"), MarketDataPreference.HYPERLIQUID, 40, null)
        r.clearDirectPending(setOf(eth))
        assertEquals(setOf(eth), r.publish(mapOf(btc to "101", eth to "11"), 50)!!.keys)
        assertEquals(emptyMap<String, PriceDeliveryDiagnostic>(), r.publish(emptyMap(), 60))
        r.clear(); assertNull(r.publish(mapOf(btc to "101"), 70))
    }
    @Test fun observerScopeIsExplicitAndBounded() {
        assertThrows(IllegalArgumentException::class.java) { PriceDeliveryRecorder(setOf("BTC")) }
        assertThrows(IllegalArgumentException::class.java) { PriceDeliveryRecorder((0..512).map { "hl:0:ASSET$it" }.toSet()) }
        PriceDeliveryRecorder((0..511).map { "hl:0:ASSET$it" }.toSet())
    }
    @Test fun cachedValuesAreNotRestampedAndObservationOpensNoSocket() = runBlocking {
        val manager = WebSocketManager("http://localhost:1", "test", "test", OkHttpClient())
        manager.publicSourceFactory = { _, _ -> error("Diagnostics cannot subscribe") }
        fun arca(prices: String) = manager.injectMessage("""{"type":"mids.updated","mids":{$prices}}""")
        arca("\"$btc\":\"100\"")
        val snapshots = Channel<Map<String, PriceDeliveryDiagnostic>>(Channel.UNLIMITED)
        val job = launch(start = CoroutineStart.UNDISPATCHED) { manager.watchPriceDeliveryDiagnostics(setOf(btc, eth)).collect { snapshots.send(it) } }
        suspend fun next() = withTimeout(2000) { snapshots.receive() }
        assertTrue(next().isEmpty())
        arca("\"$btc\":\"100\",\"$eth\":\"10\"")
        val first = next(); assertEquals(setOf(eth), first.keys)
        arca("\"$btc\":\"101\"")
        val second = next(); assertEquals(first[eth], second[eth]); assertEquals("101", second[btc]!!.price)
        assertTrue(second[btc]!!.publishedAtNanos >= second[btc]!!.receivedAtNanos)
        manager.unsubscribeMids(); assertTrue(next().isEmpty())
        job.cancelAndJoin(); manager.disconnect()
    }
    @Test fun directBatchKeepsRawReceiptAndRejectsFailedEpochWhileUnchangedQuoteProvesRecovery() = runBlocking {
        val manager = WebSocketManager("http://localhost:1", "test", "test", OkHttpClient())
        lateinit var receive: (Long, PublicMarketUpdate) -> Unit
        manager.publicSourceFactory = { _, callback -> receive = callback; object : PublicMarketSource {
            override fun subscribe(subscriptions: Set<PublicMarketSubscription>) {}
            override fun close() {}
        } }
        val owner = manager.registerPriceMarkets(); manager.updatePriceMarkets(owner, setOf(btc))
        val market = arcaJson.decodeFromString<Market>("""{"name":"hl:0:BTC","venueSymbol":"BTC","symbol":"BTC","exchange":"hl","index":0,"szDecimals":5,"maxLeverage":50,"onlyIsolated":false}""")
        manager.configureMarketData(manager.beginMarketDataConfiguration(), MarketDataPreference.HYPERLIQUID, HyperliquidNetwork.MAINNET, listOf(market))
        val snapshots = Channel<Map<String, PriceDeliveryDiagnostic>>(Channel.UNLIMITED)
        val job = launch(start = CoroutineStart.UNDISPATCHED) { manager.watchPriceDeliveryDiagnostics(setOf(btc)).collect { snapshots.send(it) } }
        suspend fun next() = withTimeout(2000) { snapshots.receive() }
        next()
        val now = System.currentTimeMillis()
        fun quote(epoch: Long, price: String, receipt: Long) = receive(epoch, PublicMarketUpdate.Received(PublicMarketUpdate.Quote(btc, price, now), receipt))
        quote(1, "100", 100); val initial = next().getValue(btc)
        quote(1, "101", 200); quote(1, "102", 300); manager.flushPublicPrices()
        val batch = next().getValue(btc)
        assertEquals("102", batch.price); assertEquals(300L, batch.receivedAtNanos); assertEquals(now, batch.venueTimeMs)
        quote(1, "999", 400); receive(2, PublicMarketUpdate.Unavailable("offline"))
        quote(1, "998", 500); manager.flushPublicPrices()
        val recoveryUpdates = manager.watchMarketDataDiagnostics()
        val recoveryJob = launch(start = CoroutineStart.UNDISPATCHED) { recoveryUpdates.collect {} }
        receive(3, PublicMarketUpdate.Connection(attemptAtNanos = 600))
        quote(3, "102", 700) // accepted but unchanged: no new display record
        manager.injectMessage("""{"type":"mids.updated","mids":{"hl:0:ETH":"10"}}""")
        assertEquals(700L, recoveryUpdates.value.hyperliquidConnection.firstPriceAtNanos,
            "first accepted unchanged quote must notify immediately, without waiting for another frame")
        recoveryJob.cancelAndJoin()
        quote(3, "103", 800); manager.flushPublicPrices()
        val recovered = next().getValue(btc)
        assertEquals("103", recovered.price); assertEquals(800L, recovered.receivedAtNanos)
        assertEquals(initial.revision + 2, recovered.revision)
        manager.setMarketDataBudget(MarketDataBudget(100_000.0, 0.4))
        receive(3, PublicMarketUpdate.Received(PublicMarketUpdate.Mids("", mapOf("BTC" to "104")), 900))
        manager.flushPublicPrices()
        val coarse = next().getValue(btc)
        assertEquals("104", coarse.price); assertEquals(900L, coarse.receivedAtNanos); assertNull(coarse.venueTimeMs)
        manager.beginMarketDataConfiguration(); quote(3, "999", 1000)
        manager.injectMessage("""{"type":"mids.updated","mids":{"hl:0:BTC":"105"}}""")
        val fallback = next().getValue(btc)
        assertEquals("105", fallback.price); assertEquals(MarketDataPreference.ARCA, fallback.source); assertNull(fallback.venueTimeMs)
        job.cancelAndJoin(); manager.disconnect()
    }
    @Test fun recoveryTimelineSurvivesLateUpgradeCallbackAndNewAttemptResetsIt() {
        val r = MarketDataDiagnosticsRecorder(); val router = MarketDataRouter()
        r.connection(PublicMarketUpdate.Connection(attemptAtNanos = 100))
        r.response(150); r.acceptedPrice(160)
        r.connection(PublicMarketUpdate.Connection(attemptAtNanos = 100, openedAtNanos = 140))
        r.connection(PublicMarketUpdate.Connection(attemptAtNanos = 100, retryAtNanos = 200, retryDelayMs = 4000))
        var s = r.snapshot(router).hyperliquidConnection
        assertEquals(140L, s.openedAtNanos); assertEquals(150L, s.firstMessageAtNanos); assertEquals(160L, s.firstPriceAtNanos)
        assertEquals(4000L, s.lastRetryDelayMs)
        r.connection(PublicMarketUpdate.Connection(attemptAtNanos = 300))
        r.connection(PublicMarketUpdate.Connection(attemptAtNanos = 100, openedAtNanos = 140))
        s = r.snapshot(router).hyperliquidConnection
        assertEquals(300L, s.attemptAtNanos); assertNull(s.openedAtNanos); assertNull(s.firstPriceAtNanos); assertEquals(4000L, s.lastRetryDelayMs)
        r.suspend(); assertNull(r.snapshot(router).hyperliquidConnection.attemptAtNanos)
    }
}
