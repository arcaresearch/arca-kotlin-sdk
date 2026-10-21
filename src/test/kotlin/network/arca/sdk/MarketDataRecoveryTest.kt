package network.arca.sdk

import kotlinx.coroutines.*
import kotlinx.coroutines.test.*
import okhttp3.*
import okio.ByteString
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

@OptIn(ExperimentalCoroutinesApi::class)
class MarketDataRecoveryTest {
    @Test fun advanceIsBoundedNeverPostponesAndKeepsCooldownAcrossFailures() {
        val window = MarketRetryWindow()
        assertNull(window.advance(0, 0))
        val first = window.schedule(0, 16_000)
        val advanced = window.advance(1_000_000_000, 249)!!
        assertEquals(499, advanced.delayMs)
        assertFalse(window.take(first.token))
        assertNull(window.advance(2_000_000_000, 0))
        assertTrue(window.take(advanced.token))
        window.schedule(3_000_000_000, 60_000)
        assertNull(window.advance(30_999_999_999, 0))
        assertEquals(250, window.advance(31_000_000_000, 0)!!.delayMs)
    }
    @Test fun imminentRetryDoesNotConsumeHintAndCancelledTimerCannotReappear() {
        val window = MarketRetryWindow()
        window.schedule(0, 200)
        assertNull(window.advance(0, 0))
        val old = window.schedule(0, 4_000)
        val advanced = window.advance(0, 0)!!
        window.cancel()
        assertFalse(window.take(old.token)); assertFalse(window.take(advanced.token))
        assertNull(window.advance(31_000_000_000, 0))
    }

    private class Socket(val listener: WebSocketListener, private val req: Request) : WebSocket {
        var cancelled = false
        override fun request() = req
        override fun queueSize() = 0L
        override fun send(text: String) = !cancelled
        override fun send(bytes: ByteString) = !cancelled
        override fun close(code: Int, reason: String?) = true
        override fun cancel() { cancelled = true }
        fun fail() = listener.onFailure(this, java.io.IOException("offline"), null)
        fun open() = listener.onOpen(this, Response.Builder().request(req).protocol(Protocol.HTTP_1_1).code(101).message("open").build())
    }
    @Test fun sourceAdvancesOnlyWaitingRetryAndPreservesBackoff() = runTest {
        val sockets = mutableListOf<Socket>()
        val timings = mutableListOf<PublicMarketUpdate.Connection>()
        val scope = CoroutineScope(SupervisorJob() + StandardTestDispatcher(testScheduler))
        val source = HyperliquidMarketSource("wss://localhost/ws", { _, update ->
            if (update is PublicMarketUpdate.Connection) timings += update
        }, WebSocket.Factory { request, listener -> Socket(listener, request).also(sockets::add) }, scope,
            nowNanos = { testScheduler.currentTime * 1_000_000 })
        try {
            source.networkAvailable(); runCurrent(); assertTrue(sockets.isEmpty())
            source.subscribe(setOf(PublicMarketSubscription("hl:0:BTC", "BTC")))
            source.networkAvailable(); runCurrent(); assertEquals(1, sockets.size) // connecting
            sockets[0].open(); source.networkAvailable(); runCurrent(); assertEquals(1, sockets.size) // live
            sockets[0].fail(); runCurrent()
            assertTrue(timings.last().retryDelayMs!! in 2_000..2_499)
            source.networkAvailable(); runCurrent()
            val shortened = timings.last().retryDelayMs!!
            assertTrue(shortened in 250..499)
            repeat(10) { source.networkAvailable() }
            advanceTimeBy(500); runCurrent(); assertEquals(2, sockets.size)
            sockets[1].fail(); runCurrent()
            assertTrue(timings.last().retryDelayMs!! in 4_000..4_499) // hint did not reset failures
            source.networkAvailable(); advanceTimeBy(2_000); runCurrent()
            assertEquals(2, sockets.size) // cooldown, and original cancelled timer did not dial
            source.subscribe(emptySet()); advanceTimeBy(60_000); runCurrent()
            assertEquals(2, sockets.size)
            source.networkAvailable(); runCurrent(); assertEquals(2, sockets.size)
        } finally { source.close() }
    }
    @Test fun closingAfterAdvancePreventsLateDial() = runTest {
        val sockets = mutableListOf<Socket>()
        val scope = CoroutineScope(SupervisorJob() + StandardTestDispatcher(testScheduler))
        val source = HyperliquidMarketSource("wss://localhost/ws", { _, _ -> },
            WebSocket.Factory { request, listener -> Socket(listener, request).also(sockets::add) }, scope,
            nowNanos = { testScheduler.currentTime * 1_000_000 })
        source.subscribe(setOf(PublicMarketSubscription("hl:0:BTC", "BTC")))
        sockets.single().fail(); source.networkAvailable(); source.close()
        source.networkAvailable(); advanceTimeBy(60_000); runCurrent()
        assertEquals(1, sockets.size)
    }
}
