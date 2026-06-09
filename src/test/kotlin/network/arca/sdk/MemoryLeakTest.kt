package network.arca.sdk

import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import network.arca.sdk.models.ObjectValuation
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * Kotlin port of Swift's `MemoryLeakTests`. The two Swift tests that assert ARC
 * deallocation via `weak var` (`testWatchFillsDoesNotRetainArca`,
 * `testOrderHandleFillsTimeoutDoesNotLeak`) are intentionally **not** ported:
 * the JVM has non-deterministic garbage collection, so a `WeakReference` +
 * `System.gc()` assertion would be flaky and prove nothing about the SDK's
 * cleanup contract. The Kotlin streams instead manage their lifetime via the
 * structured-concurrency `scope` and explicit `stop()` unsubscribe, which the
 * two behavioural tests below exercise directly.
 */
class MemoryLeakTest {

    private lateinit var server: MockWebServer

    @BeforeEach
    fun setUp() {
        server = MockWebServer()
        // Nothing here needs REST: watchPrices and watchObject are WS-only.
        // Fail fast on any stray request rather than hang.
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = MockResponse().setResponseCode(404)
        }
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    /**
     * The mids dedup logic in `watchPrices` must skip emitting an update when a
     * mids snapshot carries no change vs. the current prices box, and must emit
     * when a real value changes. (Swift: `testWatchPricesSkipsYieldWhenNoMidChanged`.)
     */
    @Test
    fun watchPricesSkipsYieldWhenNoMidChanged() = runBlocking {
        val arca = makeArca()

        // watchPrices() blocks on ready() until a mids event sets state=CONNECTED.
        // Inject the initial snapshot from a concurrent coroutine so it resolves.
        val stream = coroutineScope {
            val deferred = async { arca.watchPrices() }
            launch {
                delay(150)
                arca.ws.injectMessage(
                    """{"type":"mids.updated","mids":{"hl:0:BTC":"50000","hl:0:ETH":"3000"},"deliverySeq":1}""",
                )
            }
            withTimeout(3_000) { deferred.await() }
        }

        // Confirm the initial snapshot reached the prices box.
        assertEquals("50000", stream.prices.value["hl:0:BTC"])

        // Subscribe to updates; the collector starts AFTER the initial push, so any
        // value we observe came from a subsequent inject.
        val yields = AtomicInteger(0)
        val consumer = launch { stream.updates.collect { yields.incrementAndGet() } }
        delay(150) // let the consumer park on the iterator
        val baseline = yields.get()

        // Inject a duplicate snapshot: same keys, same values, no change.
        // The dedup logic should skip the push entirely.
        arca.ws.injectMessage(
            """{"type":"mids.updated","mids":{"hl:0:BTC":"50000","hl:0:ETH":"3000"},"deliverySeq":2}""",
        )
        delay(200)
        assertEquals(baseline, yields.get(), "duplicate mids snapshot should not produce a yield")

        // Inject a real change: BTC moves. This should yield.
        arca.ws.injectMessage(
            """{"type":"mids.updated","mids":{"hl:0:BTC":"50100"},"deliverySeq":3}""",
        )
        delay(200)
        assertTrue(yields.get() > baseline, "real mid change should produce a yield")

        consumer.cancel()
        stream.stop()
        arca.close()
    }

    /**
     * Stopping a merged `watchObjects` stream must remove its `onUpdate`
     * callbacks from each child stream, so a later child push no longer mutates
     * the merged snapshot. (Swift: `testMergedWatchObjectsUnsubscribesChildren`,
     * verified behaviourally rather than by inspecting the private callback
     * registry.)
     */
    @Test
    fun mergedWatchObjectsUnsubscribesChildren() = runBlocking {
        val arca = makeArca()

        val merged = withTimeout(3_000) { arca.watchObjects(paths = listOf("obj_1", "obj_2")) }
        val child1 = merged.childStreams[0]
        val child2 = merged.childStreams[1]

        // While subscribed, a child push propagates into the merged snapshot.
        child1.push(valuation("obj_1", "100"))
        child2.push(valuation("obj_2", "200"))
        delay(50)
        assertEquals("100", merged.valuations.value["obj_1"]?.valueUsd)
        assertEquals("200", merged.valuations.value["obj_2"]?.valueUsd)

        merged.stop()
        delay(50)

        // After stop, the merged stream has unsubscribed from its children, so a
        // fresh child push must NOT mutate the merged snapshot.
        child1.push(valuation("obj_1", "999"))
        child2.push(valuation("obj_2", "888"))
        delay(50)
        assertEquals("100", merged.valuations.value["obj_1"]?.valueUsd, "merged must ignore child 1 after stop")
        assertEquals("200", merged.valuations.value["obj_2"]?.valueUsd, "merged must ignore child 2 after stop")

        // The child streams' own state should still be reachable (sanity: stop is
        // idempotent and didn't tear down the test's ability to push).
        assertFalse(merged.childStreams.isEmpty())

        arca.close()
    }

    // MARK: - Helpers

    private fun valuation(path: String, valueUsd: String): ObjectValuation =
        ObjectValuation(objectId = ObjectId(path), path = path, type = "user", valueUsd = valueUsd)

    private fun makeArca(): Arca = Arca(token = fakeJwt(), baseUrl = server.url("/").toString().trimEnd('/'))

    private fun fakeJwt(): String {
        val enc = Base64.getUrlEncoder().withoutPadding()
        val header = enc.encodeToString("""{"alg":"HS256","typ":"JWT"}""".toByteArray())
        val payload = enc.encodeToString("""{"realmId":"rlm_test","sub":"usr_test"}""".toByteArray())
        return "$header.$payload.fakesig"
    }
}
