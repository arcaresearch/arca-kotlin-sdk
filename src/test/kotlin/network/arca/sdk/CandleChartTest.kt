package network.arca.sdk

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import network.arca.sdk.models.Candle
import network.arca.sdk.models.CandleChartUpdate
import network.arca.sdk.models.CandleInterval
import okhttp3.OkHttpClient
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger

/**
 * Ports the deterministic core of Swift's `CandleChartTests`: candle-array math
 * (`dedupCandles`/`applyCandle`), [CoverageTracker], [CandleCdn] chunk math, the
 * `CandleChartStream.onUpdate` callback registry, and CDN-fetch cancellation.
 *
 * The Swift-only `SendableBox` concurrency-box tests are omitted (Kotlin uses
 * `MutableStateFlow`/atomics). The live `watchCandleChart` `ensureRange`
 * coalescing/retry integration tests live in [CandleChartWatchTest].
 */
class CandleChartTest {

    private fun candle(t: Long, c: String): Candle =
        Candle(t = t, o = "100", h = "200", l = "50", c = c, v = "1000", n = 10, s = null)

    // MARK: - CandleInterval.milliseconds

    @Test
    fun intervalMilliseconds() {
        assertEquals(15_000, CandleInterval.FIFTEEN_SECONDS.milliseconds)
        assertEquals(60_000, CandleInterval.ONE_MINUTE.milliseconds)
        assertEquals(300_000, CandleInterval.FIVE_MINUTES.milliseconds)
        assertEquals(900_000, CandleInterval.FIFTEEN_MINUTES.milliseconds)
        assertEquals(3_600_000, CandleInterval.ONE_HOUR.milliseconds)
        assertEquals(14_400_000, CandleInterval.FOUR_HOURS.milliseconds)
        assertEquals(86_400_000, CandleInterval.ONE_DAY.milliseconds)
    }

    @Test
    fun allIntervalsHavePositiveMilliseconds() {
        for (interval in CandleInterval.entries) {
            assertTrue(interval.milliseconds > 0, "$interval should have positive milliseconds")
        }
    }

    // MARK: - dedupCandles

    @Test
    fun dedupPreservesOrderedUnique() {
        val result = dedupCandles(listOf(candle(1000, "100"), candle(2000, "200"), candle(3000, "300")))
        assertEquals(listOf(1000L, 2000L, 3000L), result.map { it.t })
    }

    @Test
    fun dedupKeepsLastForDuplicateTimestamp() {
        val result = dedupCandles(listOf(candle(1000, "100"), candle(1000, "150")))
        assertEquals(1, result.size)
        assertEquals("150", result[0].c)
    }

    @Test
    fun dedupSortsUnsortedInput() {
        val result = dedupCandles(listOf(candle(3000, "300"), candle(1000, "100"), candle(2000, "200")))
        assertEquals(listOf(1000L, 2000L, 3000L), result.map { it.t })
    }

    @Test
    fun dedupHandlesEmpty() {
        assertEquals(0, dedupCandles(emptyList()).size)
    }

    @Test
    fun dedupHandlesSingleElement() {
        val result = dedupCandles(listOf(candle(5000, "500")))
        assertEquals(1, result.size)
        assertEquals(5000L, result[0].t)
    }

    @Test
    fun dedupMergesHistoryAndLive() {
        val history = listOf(candle(1000, "100"), candle(2000, "200"), candle(3000, "300"))
        val live = listOf(candle(3000, "350"), candle(4000, "400"))
        val result = dedupCandles(history + live)
        assertEquals(4, result.size)
        assertEquals("350", result[2].c) // live wins
        assertEquals(4000L, result[3].t)
    }

    // MARK: - applyCandle

    @Test
    fun applyUpdateInPlace() {
        val arr = mutableListOf(candle(1000, "100"), candle(2000, "200"))
        applyCandle(candle(2000, "250"), arr)
        assertEquals(2, arr.size)
        assertEquals("250", arr[1].c)
    }

    @Test
    fun applyAppendNewCandle() {
        val arr = mutableListOf(candle(1000, "100"), candle(2000, "200"))
        applyCandle(candle(3000, "300"), arr)
        assertEquals(3, arr.size)
        assertEquals(3000L, arr[2].t)
        assertEquals("300", arr[2].c)
    }

    @Test
    fun applyToEmpty() {
        val arr = mutableListOf<Candle>()
        applyCandle(candle(1000, "100"), arr)
        assertEquals(1, arr.size)
        assertEquals(1000L, arr[0].t)
    }

    @Test
    fun applySequentialUpdatesAndCloses() {
        val arr = mutableListOf(candle(1000, "100"))
        applyCandle(candle(1000, "110"), arr)
        assertEquals(1, arr.size); assertEquals("110", arr[0].c)
        applyCandle(candle(1000, "120"), arr)
        assertEquals(1, arr.size); assertEquals("120", arr[0].c)
        applyCandle(candle(2000, "200"), arr)
        assertEquals(2, arr.size); assertEquals("200", arr[1].c)
        applyCandle(candle(2000, "210"), arr)
        assertEquals(2, arr.size); assertEquals("210", arr[1].c)
    }

    @Test
    fun applyOutOfOrderUpdateExisting() {
        val arr = mutableListOf(candle(1000, "100"), candle(2000, "200"), candle(3000, "300"))
        applyCandle(candle(2000, "250"), arr)
        assertEquals(3, arr.size, "Out-of-order update must not create a duplicate")
        assertEquals("250", arr[1].c)
        assertEquals(1000L, arr[0].t)
        assertEquals(3000L, arr[2].t)
    }

    @Test
    fun applyOutOfOrderInsertNewTimestamp() {
        val arr = mutableListOf(candle(1000, "100"), candle(3000, "300"))
        applyCandle(candle(2000, "200"), arr)
        assertEquals(3, arr.size, "Missing timestamp should be inserted, not duplicated")
        assertEquals(listOf(1000L, 2000L, 3000L), arr.map { it.t })
    }

    @Test
    fun applyOutOfOrderInsertBeforeAll() {
        val arr = mutableListOf(candle(2000, "200"), candle(3000, "300"))
        applyCandle(candle(1000, "100"), arr)
        assertEquals(3, arr.size)
        assertEquals(1000L, arr[0].t)
    }

    @Test
    fun applyReconnectScenarioNoDuplicates() {
        val arr = mutableListOf(candle(1000, "100"), candle(2000, "200"), candle(3000, "300"))
        applyCandle(candle(2000, "200_closed"), arr)
        assertEquals(3, arr.size, "Must update in place, not append")
        assertEquals("200_closed", arr[1].c)
        applyCandle(candle(3000, "300_live"), arr)
        assertEquals(3, arr.size)
        assertEquals("300_live", arr[2].c)
        assertEquals(dedupCandles(arr).size, arr.size, "Array should already be free of duplicates")
    }

    // MARK: - Candle array monotonicity

    @Test
    fun applyCandleNeverShrinks() {
        val arr = (0 until 300).map { candle(it * 60_000L, "$it") }.toMutableList()
        val initialCount = arr.size
        for (i in 0 until 50) {
            applyCandle(candle(299 * 60_000L, "update_$i"), arr)
            assertTrue(arr.size >= initialCount, "In-progress update should not shrink")
        }
        for (i in 300 until 320) {
            applyCandle(candle(i * 60_000L, "$i"), arr)
            assertTrue(arr.size >= initialCount, "Append should not shrink")
        }
        assertEquals(320, arr.size)
    }

    @Test
    fun gapRecoveryMergeNeverShrinks() {
        var arr = (0 until 300).map { candle(it * 60_000L, "$it") }
        val initialCount = arr.size
        val gapCandles = (280 until 310).map { candle(it * 60_000L, "gap_$it") }
        arr = dedupCandles(arr + gapCandles)
        assertTrue(arr.size >= initialCount, "Gap recovery merge should not shrink")
        assertEquals(310, arr.size)
    }

    @Test
    fun loadMoreMergeNeverShrinks() {
        var arr = (100 until 400).map { candle(it * 60_000L, "$it") }
        val initialCount = arr.size
        val older = (0 until 120).map { candle(it * 60_000L, "old_$it") }
        arr = dedupCandles(older + arr)
        assertTrue(arr.size >= initialCount, "loadMore merge should not shrink")
        assertEquals(400, arr.size)
    }

    @Test
    fun reconnectCyclePreservesCandles() {
        val arr = (0 until 300).map { candle(it * 60_000L, "$it") }.toMutableList()
        applyCandle(candle(299 * 60_000L, "live_update"), arr)
        applyCandle(candle(300 * 60_000L, "300"), arr)
        applyCandle(candle(301 * 60_000L, "301"), arr)
        assertEquals(302, arr.size)
        val gapCandles = (290 until 305).map { candle(it * 60_000L, "gap_$it") }
        val merged = dedupCandles(arr + gapCandles)
        assertEquals(305, merged.size)
        assertEquals(0L, merged[0].t)
        assertEquals("0", merged[0].c)
    }

    // MARK: - CoverageTracker

    @Test
    fun coverageAddSingleRange() {
        val tracker = CoverageTracker()
        tracker.add(100, 200)
        assertTrue(tracker.gaps(100, 200).isEmpty(), "Covered range should have no gaps")
    }

    @Test
    fun coverageGapsEmpty() {
        val gaps = CoverageTracker().gaps(100, 200)
        assertEquals(1, gaps.size)
        assertEquals(100L, gaps[0].first)
        assertEquals(200L, gaps[0].second)
    }

    @Test
    fun coverageMergesOverlapping() {
        val tracker = CoverageTracker()
        tracker.add(100, 200)
        tracker.add(150, 300)
        assertTrue(tracker.gaps(100, 300).isEmpty(), "Overlapping ranges should merge")
    }

    @Test
    fun coverageMergesAdjacent() {
        val tracker = CoverageTracker()
        tracker.add(100, 200)
        tracker.add(201, 300)
        assertTrue(tracker.gaps(100, 300).isEmpty(), "Adjacent ranges should merge")
    }

    @Test
    fun coverageGapBetweenTwoRanges() {
        val tracker = CoverageTracker()
        tracker.add(100, 200)
        tracker.add(300, 400)
        val gaps = tracker.gaps(100, 400)
        assertEquals(1, gaps.size)
        assertEquals(201L, gaps[0].first)
        assertEquals(299L, gaps[0].second)
    }

    @Test
    fun coverageGapsAtBothEdges() {
        val tracker = CoverageTracker()
        tracker.add(200, 300)
        val gaps = tracker.gaps(100, 400)
        assertEquals(2, gaps.size)
        assertEquals(100L, gaps[0].first); assertEquals(199L, gaps[0].second)
        assertEquals(301L, gaps[1].first); assertEquals(400L, gaps[1].second)
    }

    @Test
    fun coverageIdempotentAdd() {
        val tracker = CoverageTracker()
        tracker.add(100, 200)
        tracker.add(100, 200)
        assertTrue(tracker.gaps(100, 200).isEmpty(), "Duplicate add should be idempotent")
    }

    @Test
    fun coverageMultipleGaps() {
        val tracker = CoverageTracker()
        tracker.add(100, 200)
        tracker.add(400, 500)
        tracker.add(700, 800)
        val gaps = tracker.gaps(0, 1000)
        assertEquals(4, gaps.size)
        assertEquals(0L to 99L, gaps[0])
        assertEquals(201L to 399L, gaps[1])
        assertEquals(501L to 699L, gaps[2])
        assertEquals(801L to 1000L, gaps[3])
    }

    @Test
    fun coverageQuerySubsetOfCovered() {
        val tracker = CoverageTracker()
        tracker.add(100, 500)
        assertTrue(tracker.gaps(200, 400).isEmpty(), "Query within covered range should have no gaps")
    }

    @Test
    fun coverageOutOfOrderAdds() {
        val tracker = CoverageTracker()
        tracker.add(500, 600)
        tracker.add(100, 200)
        tracker.add(300, 400)
        val gaps = tracker.gaps(100, 600)
        assertEquals(2, gaps.size)
        assertEquals(201L to 299L, gaps[0])
        assertEquals(401L to 499L, gaps[1])
    }

    @Test
    fun coverageGapsReversedRange() {
        val tracker = CoverageTracker()
        tracker.add(100, 200)
        assertTrue(tracker.gaps(200, 100).isEmpty(), "Reversed range should return no gaps")
    }

    // MARK: - LoadRangeResult

    @Test
    fun loadRangeResultFields() {
        val result = network.arca.sdk.models.LoadRangeResult(
            loadedCount = 42, totalCount = 300, rangeStart = 1000, rangeEnd = 5000, reachedStart = true,
        )
        assertEquals(42, result.loadedCount)
        assertEquals(300, result.totalCount)
        assertEquals(1000L, result.rangeStart)
        assertEquals(5000L, result.rangeEnd)
        assertTrue(result.reachedStart)
    }

    // MARK: - CandleChartStream.onUpdate

    @Test
    fun candleChartStreamOnUpdateCallback() {
        val stream = CandleChartStream()
        val received = mutableListOf<CandleChartUpdate>()
        val unsub = stream.onUpdate { received.add(it) }

        val c = candle(1000, "100")
        stream.push(CandleChartUpdate(candles = listOf(c), latestCandle = c))
        assertEquals(1, received.size)
        assertEquals(1, received[0].candles.size)
        assertEquals(1000L, received[0].latestCandle.t)

        unsub()
        stream.push(CandleChartUpdate(candles = listOf(c), latestCandle = c))
        assertEquals(1, received.size, "Unsubscribed callback must not fire")
    }

    @Test
    fun candleChartStreamMultipleOnUpdateCallbacks() {
        val stream = CandleChartStream()
        var firstCount = 0
        var secondCount = 0
        stream.onUpdate { firstCount++ }
        val unsub2 = stream.onUpdate { secondCount++ }

        val c = candle(1000, "100")
        val update = CandleChartUpdate(candles = listOf(c), latestCandle = c)
        stream.push(update)
        assertEquals(1, firstCount)
        assertEquals(1, secondCount)

        unsub2()
        stream.push(update)
        assertEquals(2, firstCount)
        assertEquals(1, secondCount, "Second callback should not fire after unsub")
    }

    // MARK: - Sparse initial data (skipBackfill coverage)

    @Test
    fun sparseInitialDataDoesNotMarkCoverage() {
        val coverage = CoverageTracker()
        val count = 300
        val startTime = 0L
        val endTime = 300L * 60_000
        val sparseCandles = listOf(candle(150 * 60_000L, "150"))
        val needsRetry = sparseCandles.size < count / 2
        assertTrue(needsRetry, "1 candle out of 300 should trigger retry")
        if (!needsRetry && sparseCandles.isNotEmpty()) coverage.add(startTime, endTime)
        val gaps = coverage.gaps(startTime, endTime)
        assertEquals(1, gaps.size, "Sparse data must not mark coverage")
        assertEquals(startTime, gaps[0].first)
        assertEquals(endTime, gaps[0].second)
    }

    @Test
    fun sufficientInitialDataMarksCoverage() {
        val coverage = CoverageTracker()
        val count = 300
        val startTime = 0L
        val endTime = 300L * 60_000
        val candles = (0 until 200).map { candle(it * 60_000L, "$it") }
        val needsRetry = candles.size < count / 2
        assertFalse(needsRetry, "200 candles out of 300 should not trigger retry")
        if (!needsRetry && candles.isNotEmpty()) coverage.add(startTime, endTime)
        assertTrue(coverage.gaps(startTime, endTime).isEmpty(), "Sufficient data should mark coverage")
    }

    @Test
    fun boundaryExactlyHalfDoesNotTriggerRetry() {
        val count = 300
        val threshold = count / 2
        val candles = (0 until threshold).map { candle(it * 60_000L, "$it") }
        assertFalse(candles.size < count / 2, "Exactly count/2 candles should not trigger retry")
    }

    @Test
    fun boundaryOneUnderHalfTriggersRetry() {
        val count = 300
        val threshold = count / 2 - 1
        val candles = (0 until threshold).map { candle(it * 60_000L, "$it") }
        assertTrue(candles.size < count / 2, "count/2 - 1 candles should trigger retry")
    }

    @Test
    fun emptyInitialDataTriggersRetry() {
        val count = 300
        val candles = emptyList<Candle>()
        assertTrue(candles.size < count / 2, "Empty candles should trigger retry")
    }

    // MARK: - CandleCdn chunk boundary tests (DST spring-forward)

    @Test
    fun dailyChunkOnDSTDay() {
        val march8Ms = 1_772_928_000_000L
        val march9Ms = 1_773_014_400_000L
        val chunk = CandleCdn.chunkForTime(CandleInterval.FIVE_MINUTES, march8Ms)
        assertEquals("2026-03-08", chunk.key)
        assertEquals(march8Ms, chunk.startMs)
        assertEquals(march9Ms, chunk.endMs, "Daily chunk end must be March 9 00:00 UTC regardless of DST")
    }

    @Test
    fun weeklyChunkAcrossDST() {
        val march2Ms = 1_772_409_600_000L
        val march9Ms = 1_773_014_400_000L
        val chunk = CandleCdn.chunkForTime(CandleInterval.ONE_HOUR, march2Ms)
        assertEquals("2026-W10", chunk.key)
        assertEquals(march2Ms, chunk.startMs)
        assertEquals(march9Ms, chunk.endMs, "Weekly chunk end must be March 9 00:00 UTC despite DST")
    }

    @Test
    fun monthlyChunkAcrossDST() {
        val march1Ms = 1_772_323_200_000L
        val april1Ms = 1_775_001_600_000L
        val chunk = CandleCdn.chunkForTime(CandleInterval.ONE_DAY, march1Ms)
        assertEquals("2026-03", chunk.key)
        assertEquals(march1Ms, chunk.startMs)
        assertEquals(april1Ms, chunk.endMs, "Monthly chunk end must be April 1 00:00 UTC despite DST")
    }

    // MARK: - CandleCdn chunk boundary tests (DST fall-back)

    @Test
    fun dailyChunkOnFallBack() {
        val nov1Ms = 1_793_491_200_000L
        val nov2Ms = 1_793_577_600_000L
        val chunk = CandleCdn.chunkForTime(CandleInterval.FIVE_MINUTES, nov1Ms)
        assertEquals("2026-11-01", chunk.key)
        assertEquals(nov1Ms, chunk.startMs)
        assertEquals(nov2Ms, chunk.endMs, "Daily chunk end must be Nov 2 00:00 UTC regardless of fall-back DST")
    }

    @Test
    fun weeklyChunkAcrossFallBack() {
        val oct26Ms = 1_792_972_800_000L
        val nov2Ms = 1_793_577_600_000L
        val chunk = CandleCdn.chunkForTime(CandleInterval.ONE_HOUR, oct26Ms)
        assertEquals("2026-W44", chunk.key)
        assertEquals(oct26Ms, chunk.startMs)
        assertEquals(nov2Ms, chunk.endMs, "Weekly chunk end must be Nov 2 00:00 UTC despite fall-back DST")
    }

    @Test
    fun monthlyChunkAcrossFallBack() {
        val nov1Ms = 1_793_491_200_000L
        val dec1Ms = 1_796_083_200_000L
        val chunk = CandleCdn.chunkForTime(CandleInterval.ONE_DAY, nov1Ms)
        assertEquals("2026-11", chunk.key)
        assertEquals(nov1Ms, chunk.startMs)
        assertEquals(dec1Ms, chunk.endMs, "Monthly chunk end must be Dec 1 00:00 UTC despite fall-back DST")
    }

    // MARK: - chunksForRange termination tests

    @Test
    fun chunksForRangeWeeklyNoInfiniteLoop() {
        val march1Ms = 1_772_323_200_000L
        val april1Ms = 1_775_001_600_000L
        val chunks = CandleCdn.chunksForRange(CandleInterval.ONE_HOUR, march1Ms, april1Ms)
        assertTrue(chunks.isNotEmpty(), "Should produce at least one chunk")
        assertTrue(chunks.size <= 7, "One month should need at most 7 weekly chunks")
        for (i in 1 until chunks.size) {
            assertEquals(chunks[i - 1].endMs, chunks[i].startMs, "Chunks must be contiguous")
        }
        assertTrue(chunks.first().startMs <= march1Ms)
        assertTrue(chunks.last().endMs >= april1Ms)
    }

    @Test
    fun chunksForRangeMonthlyNoInfiniteLoop() {
        val jan1Ms = 1_767_225_600_000L
        val jan1_2027Ms = 1_798_761_600_000L
        val chunks = CandleCdn.chunksForRange(CandleInterval.ONE_DAY, jan1Ms, jan1_2027Ms)
        assertEquals(12, chunks.size, "Full year at 1d interval should produce 12 monthly chunks")
        for (i in 1 until chunks.size) {
            assertEquals(chunks[i - 1].endMs, chunks[i].startMs, "Chunks must be contiguous")
        }
        assertEquals("2026-01", chunks.first().key)
        assertEquals("2026-12", chunks.last().key)
    }

    // MARK: - Cross-SDK chunk key verification

    @Test
    fun chunkKeysMatchGoBackend() {
        val march15NoonMs = 1_773_576_000_000L
        assertEquals("2026-03-15", CandleCdn.chunkForTime(CandleInterval.FIVE_MINUTES, march15NoonMs).key)
        assertEquals("2026-W11", CandleCdn.chunkForTime(CandleInterval.ONE_HOUR, march15NoonMs).key)
        assertEquals("2026-03", CandleCdn.chunkForTime(CandleInterval.ONE_DAY, march15NoonMs).key)

        val jan1Ms = 1_767_225_600_000L
        assertEquals("2026-01-01", CandleCdn.chunkForTime(CandleInterval.ONE_MINUTE, jan1Ms).key)
        assertEquals("2026-W01", CandleCdn.chunkForTime(CandleInterval.FOUR_HOURS, jan1Ms).key)
        assertEquals("2026-01", CandleCdn.chunkForTime(CandleInterval.ONE_DAY, jan1Ms).key)
    }

    // MARK: - CandleCdn cancellation

    @Test
    fun fetchCandlesFromCdnCancellationBeforeFetchExitsImmediately() = runBlocking {
        val scope = CoroutineScope(Dispatchers.IO)
        val fallbackCalls = AtomicInteger(0)
        val job = scope.async {
            CandleCdn.fetchCandlesFromCdn(
                baseUrl = "https://cdn.example.com",
                market = "hl:0:BTC",
                interval = CandleInterval.ONE_HOUR,
                startMs = 0,
                endMs = 30L * 24 * 3_600_000,
                httpClient = OkHttpClient(),
                apiFallback = { _, _ -> fallbackCalls.incrementAndGet(); listOf(candle(1000, "100")) },
            )
        }
        job.cancel()
        val thrown = runCatching { job.await() }.exceptionOrNull()
        assertTrue(thrown is CancellationException, "Expected CancellationError, got $thrown")
        assertEquals(0, fallbackCalls.get(), "API fallback must not be called when task is cancelled")
        scope.cancel()
    }

    @Test
    fun fetchCandlesFromCdnCancellationDoesNotFallbackToApi() = runBlocking {
        val scope = CoroutineScope(Dispatchers.IO)
        val fallbackCalls = AtomicInteger(0)
        val job = scope.async {
            CandleCdn.fetchCandlesFromCdn(
                baseUrl = "http://10.255.255.1:1",
                market = "hl:0:BTC",
                interval = CandleInterval.ONE_HOUR,
                startMs = 0,
                endMs = 14L * 24 * 3_600_000,
                httpClient = OkHttpClient(),
                apiFallback = { _, _ -> fallbackCalls.incrementAndGet(); emptyList() },
            )
        }
        delay(15) // let child chunk tasks spawn and hit the network
        job.cancel()
        val thrown = runCatching { job.await() }.exceptionOrNull()
        assertTrue(thrown is CancellationException, "Expected CancellationError, got $thrown")
        assertEquals(0, fallbackCalls.get(), "API fallback must not be called after cancellation")
        scope.cancel()
    }
}
