package network.arca.sdk

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import network.arca.sdk.models.Candle
import network.arca.sdk.models.CandleChartUpdate
import network.arca.sdk.models.CandleInterval
import network.arca.sdk.models.CandlesResponse
import network.arca.sdk.models.ConnectionStatus
import network.arca.sdk.models.LoadRangeResult
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

private const val GAP_RECOVERY_CANDLES = 50

private data class PendingCandleRange(val from: Long, val to: Long)

private fun mergePendingRange(current: PendingCandleRange?, next: PendingCandleRange): PendingCandleRange {
    if (current == null) return next
    return PendingCandleRange(from = minOf(current.from, next.from), to = maxOf(current.to, next.to))
}

private class RangeLoadState {
    var loading: Boolean = false
    var pendingRange: PendingCandleRange? = null
    var task: Deferred<Int>? = null
}

// MARK: - CoverageTracker

/**
 * Tracks which time ranges have been loaded as a sorted, non-overlapping
 * interval list. Merge-on-insert keeps the list compact; gap queries against a
 * requested range run in O(n) where n is the number of coverage intervals.
 */
internal class CoverageTracker {
    private val lock = ReentrantLock()
    private val intervals = mutableListOf<Pair<Long, Long>>()

    fun add(from: Long, to: Long): Unit = lock.withLock {
        intervals.add(from to to)
        intervals.sortBy { it.first }
        val merged = mutableListOf<Pair<Long, Long>>()
        for (iv in intervals) {
            val last = merged.lastOrNull()
            if (last != null && iv.first <= last.second + 1) {
                merged[merged.size - 1] = last.first to maxOf(last.second, iv.second)
            } else {
                merged.add(iv)
            }
        }
        intervals.clear()
        intervals.addAll(merged)
    }

    fun gaps(from: Long, to: Long): List<Pair<Long, Long>> = lock.withLock {
        if (from > to) return emptyList()
        val result = mutableListOf<Pair<Long, Long>>()
        var cursor = from
        for (iv in intervals) {
            if (cursor > to) break
            if (iv.second < cursor) continue
            if (iv.first > cursor) {
                result.add(cursor to minOf(iv.first - 1, to))
            }
            cursor = maxOf(cursor, iv.second + 1)
        }
        if (cursor <= to) result.add(cursor to to)
        result
    }
}

// MARK: - candle array helpers

/**
 * Apply a single candle to a sorted array. Updates in place if the timestamp
 * already exists, appends if newer, or inserts at the correct sorted position.
 * Never creates duplicate timestamps.
 */
internal fun applyCandle(candle: Candle, arr: MutableList<Candle>) {
    val last = arr.lastOrNull()
    if (last != null && last.t == candle.t) {
        arr[arr.size - 1] = candle
        return
    }
    if (arr.isEmpty() || candle.t > arr[arr.size - 1].t) {
        arr.add(candle)
        return
    }
    var i = arr.size - 2
    while (i >= 0) {
        if (arr[i].t == candle.t) {
            arr[i] = candle
            return
        }
        if (arr[i].t < candle.t) {
            arr.add(i + 1, candle)
            return
        }
        i -= 1
    }
    arr.add(0, candle)
}

/**
 * Sort candles by timestamp and deduplicate, keeping the last entry for each
 * timestamp (live data wins over historical).
 */
internal fun dedupCandles(candles: List<Candle>): List<Candle> {
    if (candles.isEmpty()) return emptyList()
    val sorted = candles.sortedBy { it.t }
    val result = mutableListOf(sorted[0])
    for (i in 1 until sorted.size) {
        if (sorted[i].t == result[result.size - 1].t) {
            result[result.size - 1] = sorted[i]
        } else {
            result.add(sorted[i])
        }
    }
    return result
}

// MARK: - watchCandleChart

/**
 * Create a live candle chart that merges historical candle data with real-time
 * WebSocket updates. The candle array stays sorted and deduped; new bars appear
 * automatically as candle events arrive. On WebSocket reconnection, recent
 * candles are refetched to fill any gap.
 *
 * Use [CandleChartStream.ensureRange] when the visible viewport changes (zoom,
 * resize, jump to date). Use [CandleChartStream.loadMore] for simple backward
 * scrolling. Call [CandleChartStream.stop] when done.
 *
 * The `updates` stream is buffered to the latest snapshot only: slow consumers
 * drop intermediate snapshots rather than accumulating them. The full candle
 * array is always available on [CandleChartStream.candles].
 *
 * @param market Canonical market ID (e.g. `"hl:0:BTC"`, `"hl:1:BRENTOIL"`).
 * @param interval Candle interval (e.g. [CandleInterval.ONE_MINUTE]).
 * @param count Number of historical candles to load (default 300).
 */
public suspend fun Arca.watchCandleChart(
    market: String,
    interval: CandleInterval,
    count: Int = 300,
): CandleChartStream {
    ws.ensureConnected()

    val stream = CandleChartStream()
    val lock = ReentrantLock()
    val candles = mutableListOf<Candle>()
    val coverage = CoverageTracker()
    val stopped = AtomicBoolean(false)
    val reachedStart = AtomicBoolean(false)
    val previousCount = AtomicInteger(0)
    val rangeState = RangeLoadState()
    val rangeLock = ReentrantLock()
    val jobs = mutableListOf<Job>()

    ws.acquireCandles(listOf(market), listOf(interval))

    val nowMs = System.currentTimeMillis()
    val startTime = nowMs - interval.milliseconds * count
    var initialHistoryError: Throwable? = null
    val history: CandlesResponse = try {
        getCandles(market = market, interval = interval, startTime = startTime, skipBackfill = true)
    } catch (c: CancellationException) {
        ws.releaseCandles(listOf(market), listOf(interval))
        throw c
    } catch (e: Throwable) {
        initialHistoryError = e
        log.error("candle", e, mapOf("market" to market, "interval" to interval.wire, "fingerprint" to "initial_getcandles_failed")) {
            "initial getCandles failed; showing empty history"
        }
        CandlesResponse(market = market, interval = interval.wire, candles = emptyList())
    }

    val needsRetry = history.candles.size < count / 2
    val initialHistoryState: InitialHistoryState = when {
        initialHistoryError != null -> InitialHistoryState.Failed(initialHistoryError.toString())
        needsRetry && history.candles.isEmpty() -> InitialHistoryState.Failed("Empty history response")
        else -> InitialHistoryState.Loaded(history.candles.size)
    }

    val initialCandles = dedupCandles(history.candles)
    lock.withLock { candles.addAll(initialCandles) }
    previousCount.set(initialCandles.size)
    stream.historySnapshotMut.value = initialHistoryState
    stream.candlesMut.value = initialCandles
    stream.setState(WatchStreamState.CONNECTED)

    // Emit the initial history snapshot through `updates`/`onUpdate` so
    // consumers that only listen to the update stream render the chart without
    // waiting for the first live candle. Mirrors the Swift SDK, where the
    // initial snapshot is yielded into the buffered AsyncStream at creation;
    // the snapshot flow's `replay = 1` keeps it for late collectors.
    initialCandles.lastOrNull()?.let { last ->
        stream.push(CandleChartUpdate(candles = initialCandles, latestCandle = last))
    }

    if (!needsRetry && history.candles.isNotEmpty()) {
        coverage.add(startTime, nowMs)
    }

    // Emit a snapshot to `updates`/callbacks unless the candle array shrank
    // (which would flicker the chart). Always reflects in `candlesMut` via push.
    fun yieldSnapshot(snapshot: List<Candle>, trigger: Candle) {
        val size = snapshot.size
        val prev = previousCount.get()
        if (size < prev) {
            log.warning("candle", null, mapOf("previousCount" to prev.toString(), "count" to size.toString())) {
                "candle array shrunk; skipping emit to avoid flicker"
            }
            return
        }
        previousCount.updateAndGet { maxOf(it, size) }
        stream.push(CandleChartUpdate(candles = snapshot, latestCandle = trigger))
    }

    fun mergeCandles(incoming: List<Candle>): List<Candle> = lock.withLock {
        candles.addAll(incoming)
        val deduped = dedupCandles(candles)
        candles.clear()
        candles.addAll(deduped)
        deduped
    }

    fun snapshotCandles(): List<Candle> = lock.withLock { candles.toList() }

    fun makeRangeResult(loaded: Int): LoadRangeResult {
        val cands = snapshotCandles()
        return LoadRangeResult(
            loadedCount = loaded,
            totalCount = cands.size,
            rangeStart = cands.firstOrNull()?.t ?: 0L,
            rangeEnd = cands.lastOrNull()?.t ?: 0L,
            reachedStart = reachedStart.get(),
        )
    }

    // Refetch the last `GAP_RECOVERY_CANDLES` bars. Shared by the WS re-auth
    // path and the app-foreground path.
    suspend fun recoverGap() {
        val gapStart = System.currentTimeMillis() - interval.milliseconds * GAP_RECOVERY_CANDLES
        val res = runCatching { getCandles(market = market, interval = interval, startTime = gapStart) }
            .getOrElse { e ->
                log.warning("candle", e, mapOf("market" to market, "interval" to interval.wire)) { "gap recovery refetch failed" }
                return
            }
        if (res.candles.isNotEmpty()) {
            val snapshot = mergeCandles(res.candles)
            val gapEnd = System.currentTimeMillis()
            coverage.add(gapStart, gapEnd)
            snapshot.lastOrNull()?.let { yieldSnapshot(snapshot, it) }
        }
    }

    // Drains the coalesced pending range queue, fetching each coverage gap
    // concurrently and merging the results. Returns the number of candles loaded.
    suspend fun drainPendingRanges(): Int {
        var totalLoaded = 0
        while (!stopped.get()) {
            val requested = rangeLock.withLock {
                val r = rangeState.pendingRange
                rangeState.pendingRange = null
                r
            } ?: break

            val gaps = coverage.gaps(requested.from, requested.to)
            coroutineScope {
                val results = gaps.map { gap ->
                    async {
                        runCatching { getCandles(market = market, interval = interval, startTime = gap.first, endTime = gap.second) }
                            .map { Triple(gap.first, gap.second, it.candles) }
                            .getOrElse { e ->
                                log.warning("candle", e, mapOf("market" to market, "interval" to interval.wire, "from" to gap.first.toString(), "to" to gap.second.toString())) {
                                    "range gap fetch failed"
                                }
                                null
                            }
                    }
                }.awaitAll()

                for (result in results) {
                    if (stopped.get() || !isActive) return@coroutineScope
                    if (result == null) continue
                    val (gapFrom, gapTo, fetched) = result
                    if (fetched.isNotEmpty()) {
                        mergeCandles(fetched)
                        totalLoaded += fetched.size
                        coverage.add(gapFrom, gapTo)
                    } else {
                        val earliest = lock.withLock { candles.firstOrNull()?.t } ?: Long.MAX_VALUE
                        if (gapFrom <= earliest) {
                            reachedStart.set(true)
                            coverage.add(gapFrom, gapTo)
                        }
                    }
                    val snapshot = snapshotCandles()
                    snapshot.lastOrNull()?.let { yieldSnapshot(snapshot, it) }
                }
            }
            if (!currentCoroutineContext().isActive) break
        }
        return totalLoaded
    }

    suspend fun ensureRange(start: Long, end: Long): LoadRangeResult {
        if (stopped.get()) return makeRangeResult(0)
        if (start > end) return makeRangeResult(0)
        if (coverage.gaps(start, end).isEmpty()) return makeRangeResult(0)

        val requestedRange = PendingCandleRange(start, end)
        fun enqueue() = rangeLock.withLock {
            rangeState.pendingRange = mergePendingRange(rangeState.pendingRange, requestedRange)
        }
        enqueue()

        while (true) {
            var existingTask: Deferred<Int>? = null
            var shouldStartTask = false
            rangeLock.withLock {
                if (rangeState.loading) {
                    existingTask = rangeState.task
                } else {
                    rangeState.loading = true
                    shouldStartTask = true
                }
            }

            if (shouldStartTask) {
                val task = scope.async { drainPendingRanges() }
                rangeLock.withLock { rangeState.task = task }
                val loaded = task.await()
                rangeLock.withLock {
                    rangeState.loading = false
                    rangeState.task = null
                }
                return makeRangeResult(loaded)
            }

            val running = existingTask
            if (running != null) running.await() else yield()

            if (stopped.get()) return makeRangeResult(0)
            if (coverage.gaps(start, end).isEmpty()) return makeRangeResult(0)
            enqueue()
        }
    }

    suspend fun loadMore(loadCount: Int): LoadRangeResult {
        val earliest = lock.withLock { candles.firstOrNull()?.t } ?: 0L
        if (earliest <= 0L) return makeRangeResult(0)
        val end = earliest - 1
        val start = maxOf(0L, end - interval.milliseconds * loadCount)
        return ensureRange(start, end)
    }

    stream.ensureRangeAction = { s, e -> ensureRange(s, e) }
    stream.loadMoreAction = { c -> loadMore(c) }

    // Live candle events: always update the array; gate emits until history loads.
    jobs += scope.launch {
        ws.candleEvents().collect { event ->
            if (event.market != market || event.interval != interval) return@collect
            val latest = event.candle
            val snapshot: List<Candle>
            lock.withLock {
                applyCandle(latest, candles)
                snapshot = candles.toList()
            }
            stream.candlesMut.value = snapshot
            if (stream.historySnapshotMut.value is InitialHistoryState.Loaded) {
                yieldSnapshot(snapshot, latest)
            }
        }
    }

    jobs += scope.launch {
        ws.statusStream.collect { s ->
            when (s) {
                ConnectionStatus.DISCONNECTED -> stream.setState(WatchStreamState.RECONNECTING)
                ConnectionStatus.CONNECTED -> stream.setState(WatchStreamState.CONNECTED)
                else -> {}
            }
        }
    }

    // Every successful re-auth (post-reconnect) recovers the gap.
    jobs += scope.launch {
        ws.authenticatedStream.collect { recoverGap() }
    }

    // App foreground after a hidden period.
    jobs += scope.launch {
        ws.resumeStream.collect { recoverGap() }
    }

    if (needsRetry) {
        jobs += scope.launch {
            var delayMs = 1_000L
            val maxDelayMs = 30_000L
            while (isActive) {
                delay(delayMs)
                if (!isActive) return@launch
                val res = runCatching {
                    val retryStart = System.currentTimeMillis() - interval.milliseconds * count
                    getCandles(market = market, interval = interval, startTime = retryStart) to retryStart
                }.getOrElse { e ->
                    log.warning("candle", e, mapOf("market" to market, "interval" to interval.wire)) { "initial candle retry failed; backing off" }
                    null
                }
                if (res != null && res.first.candles.isNotEmpty()) {
                    val snapshot = mergeCandles(res.first.candles)
                    stream.historySnapshotMut.value = InitialHistoryState.Loaded(res.first.candles.size)
                    coverage.add(res.second, System.currentTimeMillis())
                    snapshot.lastOrNull()?.let { yieldSnapshot(snapshot, it) }
                    return@launch
                }
                delayMs = minOf(delayMs * 2, maxDelayMs)
            }
        }
    }

    stream.stopAction = {
        stopped.set(true)
        jobs.forEach { it.cancel() }
        ws.releaseCandles(listOf(market), listOf(interval))
    }

    return stream
}
