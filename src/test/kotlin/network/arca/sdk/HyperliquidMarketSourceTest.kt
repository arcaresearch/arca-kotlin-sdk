package network.arca.sdk

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import okhttp3.*
import okhttp3.mockwebserver.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class HyperliquidMarketSourceTest {
    @Test fun optionalMainnetPublicStreamProbe() = runBlocking {
        org.junit.jupiter.api.Assumptions.assumeTrue(System.getenv("ARCA_PUBLIC_STREAM_PROBE") == "1")
        val quotes = java.util.concurrent.atomic.AtomicInteger()
        val bars = java.util.concurrent.atomic.AtomicInteger()
        val failures = java.util.concurrent.atomic.AtomicInteger()
        val source = HyperliquidMarketSource(HyperliquidNetwork.MAINNET.websocketUrl, { _, update ->
            when (update) {
                is PublicMarketUpdate.Quote -> quotes.incrementAndGet()
                is PublicMarketUpdate.Bar -> bars.incrementAndGet()
                is PublicMarketUpdate.Unavailable -> failures.incrementAndGet()
            }
        })
        try {
            source.subscribe(setOf(PublicMarketSubscription("hl:0:BTC", "BTC"), PublicMarketSubscription("hl:0:BTC", "BTC", network.arca.sdk.models.CandleInterval.ONE_MINUTE)))
            delay(35_000)
            println("PUBLIC_STREAM_PROBE kotlin network=HL-mainnet duration_s=35 quotes=${quotes.get()} candles=${bars.get()} errors=${failures.get()}")
            assertTrue(quotes.get() > 0); assertTrue(bars.get() > 0); assertEquals(0, failures.get())
        } finally { source.close() }
    }

    @Test fun publicSocketNeverSendsArcaCredentialsAndSubscriptionErrorFallsBack() = runBlocking {
        val server = MockWebServer()
        val received = Channel<PublicMarketUpdate>(Channel.UNLIMITED)
        val sent = Channel<String>(Channel.UNLIMITED)
        server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
            override fun onMessage(webSocket: WebSocket, text: String) {
                sent.trySend(text)
                if (text.contains("subscribe")) {
                    webSocket.send("""{"channel":"bbo","data":{"coin":"BTC","time":1000,"bbo":[{"px":"84970"},{"px":"84971"}]}}""")
                    webSocket.send("""{"channel":"error","data":"unavailable"}""")
                }
            }
        }))
        server.start()
        val source = HyperliquidMarketSource(server.url("/ws").toString(), { _, update -> received.trySend(update) })
        try {
            source.subscribe(setOf(PublicMarketSubscription("hl:0:BTC", "BTC")))
            assertTrue(withTimeout(3000) { sent.receive() }.contains("bbo"))
            assertEquals("84970.5", (withTimeout(3000) { received.receive() } as PublicMarketUpdate.Quote).price)
            assertTrue(withTimeout(3000) { received.receive() } is PublicMarketUpdate.Unavailable)
            val request = server.takeRequest(1, java.util.concurrent.TimeUnit.SECONDS)!!
            assertNull(request.getHeader("Authorization")); assertNull(request.getHeader("Cookie"))
        } finally { source.close(); server.shutdown() }
    }
}
