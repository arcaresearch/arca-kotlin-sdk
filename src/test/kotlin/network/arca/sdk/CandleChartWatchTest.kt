package network.arca.sdk

import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import network.arca.sdk.models.CandleInterval
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Base64
import java.util.Collections
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * Ports the live `watchCandleChart` `ensureRange` integration tests from Swift's
 * `CandleChartTests` (coalescing, failed-gap retry, concurrent gap fetching, and
 * incremental snapshot emission). Uses [MockWebServer] in place of Swift's
 * `URLProtocol` stub; gap responses can be delayed or fail mid-connection.
 */
class CandleChartWatchTest {

    private lateinit var server: MockWebServer
    private lateinit var dispatcher: CandleDispatcher

    @BeforeEach
    fun setUp() {
        dispatcher = CandleDispatcher()
        server = MockWebServer()
        server.dispatcher = dispatcher
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    @Test
    fun ensureRangeCoalescesOverlappingRequests() = runBlocking {
        val arca = makeArca()
        val intervalMs = CandleInterval.ONE_MINUTE.milliseconds
        val initialStart = AtomicLong(0)

        dispatcher.handler = { request, index ->
            when (index) {
                0 -> {
                    initialStart.set(query(request, "startTime") ?: 0)
                    val s = initialStart.get()
                    CandleResp(body = envelope(s + intervalMs to "100", s + intervalMs * 2 to "200", s + intervalMs * 3 to "300"))
                }
                1 -> CandleResp(delayMs = 100, body = envelope(initialStart.get() to "90"))
                2 -> CandleResp(body = envelope(initialStart.get() - intervalMs to "80"))
                else -> CandleResp(error = true)
            }
        }

        val stream = arca.watchCandleChart(market = "hl:0:BTC", interval = CandleInterval.ONE_MINUTE, count = 3)
        val s = initialStart.get()
        val firstStart = s - intervalMs
        val firstEnd = s - 1
        val secondStart = s - intervalMs * 2

        val firstTask = async { stream.ensureRange(firstStart, firstEnd) }
        delay(20)
        val secondTask = async { stream.ensureRange(secondStart, firstEnd) }

        val first = firstTask.await()
        val second = secondTask.await()

        assertEquals(3, dispatcher.requestCount)
        assertEquals(2, first.loadedCount)
        assertEquals(5, second.totalCount)
        assertEquals(
            listOf(s - intervalMs, s, s + intervalMs, s + intervalMs * 2, s + intervalMs * 3),
            stream.candles.value.map { it.t },
        )

        assertEquals(firstStart, query(dispatcher.requests[1], "startTime"))
        assertEquals(firstEnd, query(dispatcher.requests[1], "endTime"))
        assertEquals(secondStart, query(dispatcher.requests[2], "startTime"))
        assertEquals(s - intervalMs - 1, query(dispatcher.requests[2], "endTime"))

        stream.stop()
        arca.close()
    }

    @Test
    fun ensureRangeRetriesFailedGap() = runBlocking {
        val arca = makeArca()
        val intervalMs = CandleInterval.ONE_MINUTE.milliseconds
        val initialStart = AtomicLong(0)

        dispatcher.handler = { request, index ->
            when (index) {
                0 -> {
                    initialStart.set(query(request, "startTime") ?: 0)
                    val s = initialStart.get()
                    CandleResp(body = envelope(s + intervalMs to "100", s + intervalMs * 2 to "200"))
                }
                1 -> CandleResp(error = true)
                2 -> CandleResp(body = envelope(initialStart.get() - intervalMs to "90"))
                else -> CandleResp(error = true)
            }
        }

        val stream = arca.watchCandleChart(market = "hl:0:BTC", interval = CandleInterval.ONE_MINUTE, count = 2)
        val rangeStart = initialStart.get() - intervalMs
        val rangeEnd = initialStart.get() - 1

        val first = stream.ensureRange(rangeStart, rangeEnd)
        val second = stream.ensureRange(rangeStart, rangeEnd)

        assertEquals(0, first.loadedCount)
        assertEquals(1, second.loadedCount)
        assertEquals(3, dispatcher.requestCount)
        assertEquals(initialStart.get() - intervalMs, stream.candles.value.firstOrNull()?.t)

        stream.stop()
        arca.close()
    }

    @Test
    fun ensureRangeFetchesGapsConcurrently() = runBlocking {
        val arca = makeArca()
        val intervalMs = CandleInterval.ONE_MINUTE.milliseconds
        val initialStart = AtomicLong(0)

        dispatcher.handler = { request, index ->
            if (index == 0) {
                initialStart.set(query(request, "startTime") ?: 0)
                val s = initialStart.get()
                CandleResp(body = envelope(s + intervalMs * 3 to "300", s + intervalMs * 7 to "700"))
            } else {
                val st = query(request, "startTime") ?: 0
                CandleResp(delayMs = 150, body = envelope(st + intervalMs to "$index"))
            }
        }

        val stream = arca.watchCandleChart(market = "hl:0:BTC", interval = CandleInterval.ONE_MINUTE, count = 2)
        val gapStart = initialStart.get() - intervalMs * 2
        val gapEnd = initialStart.get() + intervalMs * 10

        val start = System.currentTimeMillis()
        val result = stream.ensureRange(gapStart, gapEnd)
        val elapsed = System.currentTimeMillis() - start

        assertTrue(result.loadedCount > 0, "Should have loaded candles for the gaps")
        val requestCount = dispatcher.requestCount - 1
        assertTrue(requestCount >= 2, "Should have fetched at least 2 gaps")
        val sequentialMs = requestCount * 150L
        assertTrue(elapsed < sequentialMs, "Gaps should be fetched concurrently: ${elapsed}ms < ${sequentialMs}ms")

        stream.stop()
        arca.close()
    }

    @Test
    fun ensureRangeEmitsIncrementalSnapshots() = runBlocking {
        val arca = makeArca()
        val intervalMs = CandleInterval.ONE_MINUTE.milliseconds
        val initialStart = AtomicLong(0)

        dispatcher.handler = { request, index ->
            if (index == 0) {
                initialStart.set(query(request, "startTime") ?: 0)
                val s = initialStart.get()
                CandleResp(body = envelope(s + intervalMs to "100", s + intervalMs * 2 to "200"))
            } else {
                val st = query(request, "startTime") ?: 0
                CandleResp(delayMs = index * 50L, body = envelope(st + intervalMs to "${index * 100}"))
            }
        }

        val stream = arca.watchCandleChart(market = "hl:0:BTC", interval = CandleInterval.ONE_MINUTE, count = 2)

        val updateCounts = Collections.synchronizedList(mutableListOf<Int>())
        val unsub = stream.onUpdate { updateCounts.add(it.candles.size) }

        val gapStart = initialStart.get() - intervalMs * 3
        val gapEnd = initialStart.get() + intervalMs * 5
        stream.ensureRange(gapStart, gapEnd)

        val counts = synchronized(updateCounts) { updateCounts.toList() }
        assertTrue(counts.size >= 2, "Should have emitted at least 2 incremental snapshots, got ${counts.size}")
        for (i in 1 until counts.size) {
            assertTrue(counts[i] >= counts[i - 1], "Snapshot candle count must not decrease: ${counts[i - 1]} -> ${counts[i]}")
        }

        unsub()
        stream.stop()
        arca.close()
    }

    // MARK: - Helpers

    // candleCdnBaseUrl = null forces the direct REST candle path (no CDN chunking
    // / error-swallowing), mirroring the Swift test's `candleCdnBaseUrl: nil`.
    private fun makeArca(): Arca = Arca(
        token = fakeJwt(),
        baseUrl = server.url("/").toString().trimEnd('/'),
        candleCdnBaseUrl = null,
    )

    private fun fakeJwt(): String {
        val enc = Base64.getUrlEncoder().withoutPadding()
        val header = enc.encodeToString("""{"alg":"HS256","typ":"JWT"}""".toByteArray())
        val payload = enc.encodeToString("""{"realmId":"rlm_test","sub":"usr_test"}""".toByteArray())
        return "$header.$payload.fakesig"
    }

    private fun query(request: RecordedRequest, name: String): Long? =
        request.requestUrl?.queryParameter(name)?.toLongOrNull()

    private fun envelope(vararg candles: Pair<Long, String>): String {
        val arr = candles.joinToString(",") { (t, c) ->
            """{"t":$t,"o":"100","h":"200","l":"50","c":"$c","v":"1000","n":10}"""
        }
        return """{"success":true,"data":{"market":"hl:0:BTC","interval":"1m","candles":[$arr]}}"""
    }
}

private data class CandleResp(val delayMs: Long = 0, val body: String? = null, val error: Boolean = false)

private class CandleDispatcher : Dispatcher() {
    private val index = AtomicInteger(0)
    private val captured = Collections.synchronizedList(mutableListOf<RecordedRequest>())

    @Volatile
    var handler: ((RecordedRequest, Int) -> CandleResp)? = null

    val requests: List<RecordedRequest> get() = synchronized(captured) { captured.toList() }
    val requestCount: Int get() = requests.size

    override fun dispatch(request: RecordedRequest): MockResponse {
        val path = (request.path ?: "").substringBefore("?")
        // The WS upgrade and any non-candle path: 404, not counted as a candle request.
        if (!path.contains("/exchange/market/candles/")) {
            return MockResponse().setResponseCode(404)
        }
        val i = index.getAndIncrement()
        captured.add(request)
        val resp = handler?.invoke(request, i) ?: CandleResp(error = true)
        // A non-retryable 500 (not 502/503/504) makes getCandles throw without
        // ArcaClient or OkHttp silently retrying and consuming the next index.
        if (resp.error) return MockResponse().setResponseCode(500).setBody("""{"success":false,"error":{"code":"INTERNAL","message":"boom"}}""")
        val mr = MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody(resp.body ?: "")
        if (resp.delayMs > 0) mr.setBodyDelay(resp.delayMs, TimeUnit.MILLISECONDS)
        return mr
    }
}
