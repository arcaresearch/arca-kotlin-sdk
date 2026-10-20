package network.arca.sdk

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.OkHttpClient
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.Collections

class LatestSnapshotCalculationTest {
    @Test fun slowMidsConsumerKeepsDisjointMarketsAndNewLifetimeHasNoOldReplay() = runBlocking {
        val manager = WebSocketManager("http://localhost:1", "token", "realm", OkHttpClient())
        val received = Channel<Map<String, String>>(Channel.UNLIMITED)
        val release = CompletableDeferred<Unit>()
        var count = 0
        val consumer = launch(start = CoroutineStart.UNDISPATCHED) {
            manager.midsEvents().collect {
                received.send(it)
                if (++count == 1) release.await()
            }
        }
        manager.injectMessage("""{"type":"mids.snapshot","mids":{"A":"0","B":"0"}}""")
        assertEquals(mapOf("A" to "0", "B" to "0"), withTimeout(2000) { received.receive() })
        for (i in 1..1000) {
            val asset = if (i % 2 == 0) "A" else "B"
            manager.injectMessage("""{"type":"mids.updated","mids":{"$asset":"$i"}}""")
        }
        release.complete(Unit)
        assertEquals(mapOf("A" to "1000", "B" to "999"), withTimeout(2000) { received.receive() })
        assertTrue(received.tryReceive().isFailure)
        consumer.cancel()
        manager.unsubscribeMids()
        manager.injectMessage("""{"type":"mids.snapshot","mids":{"C":"1"}}""")
        assertEquals(mapOf("C" to "1"), withTimeout(2000) { manager.midsEvents().first() })
        manager.disconnect()
    }

    @Test fun equalPriceObservationsRemainObservableWhenConsumerKeepsUp() = runBlocking {
        val manager = WebSocketManager("http://localhost:1", "token", "realm", OkHttpClient())
        val received = Channel<Map<String, String>>(Channel.UNLIMITED)
        val consumer = launch(start = CoroutineStart.UNDISPATCHED) { manager.midsEvents().collect { received.send(it) } }
        repeat(2) {
            manager.injectMessage("""{"type":"mids.updated","mids":{"A":"1"}}""")
            assertEquals(mapOf("A" to "1"), withTimeout(2000) { received.receive() })
        }
        consumer.cancel()
        manager.disconnect()
    }

    @Test fun overloadCompletesActiveWorkThenOnlyNewestPending() = runBlocking {
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val results = Channel<Int>(Channel.UNLIMITED)
        val starts = Collections.synchronizedList(mutableListOf<Int>())
        val caller = Thread.currentThread()
        val worker = LatestSnapshotCalculation<Int, Int>(this, calculate = {
            assertNotSame(caller, Thread.currentThread())
            starts += it
            if (it == 1) { entered.complete(Unit); release.await() }
            it
        }, publish = { _, value -> results.trySend(value) })
        worker.submit(1)
        withTimeout(2000) { entered.await() }
        for (i in 2..1000) worker.submit(i)
        release.complete(Unit)
        assertEquals(1, withTimeout(2000) { results.receive() })
        assertEquals(1000, withTimeout(2000) { results.receive() })
        assertEquals(listOf(1, 1000), starts)
        worker.stop()
    }

    @Test fun stopRejectsNonCooperativeCompletionAndPendingWork() = runBlocking {
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val returned = CompletableDeferred<Unit>()
        val results = Channel<Int>(Channel.UNLIMITED)
        val starts = Collections.synchronizedList(mutableListOf<Int>())
        val worker = LatestSnapshotCalculation<Int, Int>(this, calculate = {
            starts += it
            withContext(NonCancellable) { entered.complete(Unit); release.await() }
            returned.complete(Unit)
            it
        }, publish = { _, value -> results.trySend(value) })
        worker.submit(1)
        withTimeout(2000) { entered.await() }
        worker.submit(2)
        worker.stop()
        release.complete(Unit)
        withTimeout(2000) { returned.await() }
        assertNull(withTimeoutOrNull(100) { results.receive() })
        assertEquals(listOf(1), starts)
    }
}
