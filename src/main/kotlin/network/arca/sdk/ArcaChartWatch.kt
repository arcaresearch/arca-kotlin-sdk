package network.arca.sdk

import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import network.arca.sdk.models.AggregationSource
import network.arca.sdk.models.AggregationSourceType
import network.arca.sdk.models.ChartPointStatus
import network.arca.sdk.models.ConnectionStatus
import network.arca.sdk.models.EquityHistoryResponse
import network.arca.sdk.models.EquityPoint
import network.arca.sdk.models.ExternalFlowEntry
import network.arca.sdk.models.PnlAnchor
import network.arca.sdk.models.PnlChartBaseline
import network.arca.sdk.models.PnlChartUpdate
import network.arca.sdk.models.PnlHistoryResponse
import network.arca.sdk.models.PnlPoint
import network.arca.sdk.models.applyEquityAnchor
import java.time.Instant
import java.time.OffsetDateTime
import kotlin.math.abs

// MARK: - Helpers

private fun parseInstantOrNull(s: String): Instant? =
    runCatching { Instant.parse(s) }.getOrNull()
        ?: runCatching { OffsetDateTime.parse(s).toInstant() }.getOrNull()

private fun parseDoubleOrZero(s: String?): Double = s?.toDoubleOrNull() ?: 0.0

private fun parseDoubleOrNull(s: String?): Double? = s?.toDoubleOrNull()

private fun nowEpochSeconds(): Long = Instant.now().epochSecond

private fun bucketBoundary(epochSeconds: Long, resolutionSeconds: Long): Long =
    (epochSeconds / resolutionSeconds) * resolutionSeconds

// MARK: - watchEquityChart

/**
 * Create a live equity chart that merges historical data with real-time
 * aggregation updates. The last point reflects the current live equity; when
 * the bucket boundary crosses, the live point is promoted to historical and a
 * new live point starts.
 *
 * The SDK-appended live point always carries [ChartPointStatus.OPEN] so
 * consumers can identify it explicitly (e.g. to restyle, drop, or re-anchor
 * the live tip) instead of inferring it from its timestamp.
 *
 * The stream buffers the latest value and drops intermediate updates if the
 * consumer is slow. Updates are also dropped if the live point hasn't
 * materially changed. Call [EquityChartStream.stop] when done.
 *
 * @param path Object path or path prefix (trailing slash aggregates).
 * @param from Start timestamp (RFC 3339).
 * @param to End timestamp (RFC 3339). When within ~60s of now, the window
 *   slides forward on every refresh ("live tail"); otherwise it stays pinned.
 * @param points Number of historical samples (default 1000, max 1000).
 * @param exchange Exchange identifier for mid prices (default `"sim"`).
 */
public suspend fun Arca.watchEquityChart(
    path: String,
    from: String,
    to: String,
    points: Int = 1000,
    exchange: String = "sim",
): EquityChartStream {
    validatePath(path)

    val createdAtMs = System.currentTimeMillis()
    val fromInstant = parseInstantOrNull(from)
    val toInstant = parseInstantOrNull(to)
    val isLiveTail = fromInstant != null && toInstant != null &&
        abs(toInstant.toEpochMilli() - createdAtMs) <= LIVE_TAIL_THRESHOLD_S * 1000
    val windowSeconds: Long =
        if (isLiveTail) {
            // isLiveTail already implies fromInstant/toInstant are non-null
            // (see its definition above); the compiler smart-casts them here.
            maxOf(0L, toInstant.epochSecond - fromInstant.epochSecond)
        } else {
            0L
        }

    val mutex = Mutex()
    var windowFrom = from
    var windowTo = to

    // Must be called while holding `mutex`.
    fun slideIfLiveLocked() {
        if (!isLiveTail) return
        val now = Instant.now()
        windowFrom = iso8601String(now.minusSeconds(windowSeconds))
        windowTo = iso8601String(now)
    }

    var history: EquityHistoryResponse = runCatching {
        getEquityHistory(path, windowFrom, windowTo, points)
    }.getOrElse {
        EquityHistoryResponse(prefix = path, from = from, to = to, points = 0, equityPoints = emptyList())
    }

    val aggStream = watchAggregation(
        sources = listOf(AggregationSource(type = AggregationSourceType.PREFIX, value = path)),
        exchange = exchange,
    )

    // If live equity is non-zero but cached history is all zeros (new account
    // after deposit), drop the stale cache and refetch.
    aggStream.aggregation.value?.totalEquityUsd?.toDoubleOrNull()?.let { live ->
        if (live > 0.01) {
            val allZero = history.equityPoints.isEmpty() ||
                history.equityPoints.all { abs(parseDoubleOrZero(it.equityUsd)) < 0.01 }
            if (allZero) {
                val key = buildCacheKey(
                    "equityHistory",
                    mapOf("target" to path, "kind" to "path", "from" to windowFrom, "to" to windowTo, "points" to points.toString()),
                )
                historyCache.delete(key)
                runCatching { getEquityHistory(path, windowFrom, windowTo, points) }.getOrNull()?.let { history = it }
            }
        }
    }

    var resolutionSeconds = chartResolutionSeconds(history.resolution)
    var hourBoundary = bucketBoundary(nowEpochSeconds(), resolutionSeconds)

    val trimmed = history.equityPoints.toMutableList()
    while (trimmed.isNotEmpty()) {
        val ts = parseInstantOrNull(trimmed.last().timestamp) ?: break
        if (ts.epochSecond > hourBoundary) trimmed.removeAt(trimmed.size - 1) else break
    }

    var historical = trimmed.toMutableList()
    var liveEquity: String? = aggStream.aggregation.value?.totalEquityUsd
    var lastAggAtMs = System.currentTimeMillis()

    val initialChart = trimmed.toMutableList()
    aggStream.aggregation.value?.let { agg ->
        initialChart.add(
            EquityPoint(timestamp = iso8601String(Instant.now()), equityUsd = agg.totalEquityUsd, status = ChartPointStatus.OPEN),
        )
    }

    val stream = EquityChartStream()
    stream.setState(WatchStreamState.CONNECTED)
    stream.chartMut.value = initialChart

    val chartWatchId = ws.watchChartHistory(path)
    val jobs = mutableListOf<Job>()

    // Refetch historical equity and re-emit the chart with the live tip.
    suspend fun refreshHistory() {
        val w: Pair<String, String>
        mutex.withLock {
            slideIfLiveLocked()
            w = windowFrom to windowTo
        }
        val key = buildCacheKey(
            "equityHistory",
            mapOf("target" to path, "kind" to "path", "from" to w.first, "to" to w.second, "points" to points.toString()),
        )
        historyCache.delete(key)
        val fresh = runCatching { getEquityHistory(path, w.first, w.second, points) }.getOrNull() ?: return
        val allPoints: List<EquityPoint>
        mutex.withLock {
            resolutionSeconds = chartResolutionSeconds(fresh.resolution)
            hourBoundary = bucketBoundary(nowEpochSeconds(), resolutionSeconds)
            val filtered = fresh.equityPoints.filter { it.status != ChartPointStatus.OPEN }
            historical = filtered.toMutableList()
            val pts = filtered.toMutableList()
            aggStream.aggregation.value?.let { agg ->
                liveEquity = agg.totalEquityUsd
                pts.add(EquityPoint(timestamp = iso8601String(Instant.now()), equityUsd = agg.totalEquityUsd, status = ChartPointStatus.OPEN))
            }
            allPoints = pts
        }
        stream.push(allPoints)
    }

    val gapId = ws.onGap {
        scope.launch {
            val w: Pair<String, String>
            mutex.withLock {
                slideIfLiveLocked()
                w = windowFrom to windowTo
            }
            val key = buildCacheKey(
                "equityHistory",
                mapOf("target" to path, "kind" to "path", "from" to w.first, "to" to w.second, "points" to points.toString()),
            )
            historyCache.delete(key)
            val fresh = runCatching { getEquityHistory(path, w.first, w.second, points) }.getOrNull() ?: return@launch
            mutex.withLock {
                resolutionSeconds = chartResolutionSeconds(fresh.resolution)
                hourBoundary = bucketBoundary(nowEpochSeconds(), resolutionSeconds)
                historical = fresh.equityPoints.filter { it.status != ChartPointStatus.OPEN }.toMutableList()
            }
        }
    }

    jobs += scope.launch {
        aggStream.updates.collect { agg ->
            var doRefresh = false
            var emit: List<EquityPoint>? = null
            mutex.withLock {
                lastAggAtMs = System.currentTimeMillis()
                val previousLiveEquity = liveEquity
                val live = agg.totalEquityUsd
                val currentBoundary = bucketBoundary(nowEpochSeconds(), resolutionSeconds)
                val lastBoundary = hourBoundary

                if (currentBoundary - lastBoundary > resolutionSeconds && historical.isNotEmpty()) {
                    // Multi-bucket gap: refetch the dense server window which
                    // fills every intermediate bucket.
                    doRefresh = true
                } else {
                    if (currentBoundary > lastBoundary && previousLiveEquity != null) {
                        if (historical.isNotEmpty()) {
                            historical.add(
                                EquityPoint(timestamp = iso8601String(Instant.ofEpochSecond(lastBoundary)), equityUsd = previousLiveEquity),
                            )
                        }
                        hourBoundary = currentBoundary
                    }
                    liveEquity = live

                    val allPoints = historical.toMutableList().apply {
                        add(EquityPoint(timestamp = iso8601String(Instant.now()), equityUsd = live, status = ChartPointStatus.OPEN))
                    }
                    val prevPoints = stream.chartMut.value
                    if (prevPoints.size == allPoints.size && prevPoints.lastOrNull()?.equityUsd == live) {
                        stream.chartMut.value = allPoints
                    } else {
                        emit = allPoints
                    }
                }
            }
            if (doRefresh) refreshHistory() else emit?.let { stream.push(it) }
        }
    }

    jobs += scope.launch {
        ws.chartSnapshotEvents().collect { (eventWatchId, _) ->
            if (eventWatchId == chartWatchId) refreshHistory()
        }
    }

    jobs += scope.launch {
        ws.statusStream.collect { s ->
            if (s == ConnectionStatus.DISCONNECTED && stream.state.value != WatchStreamState.LOADING) {
                stream.setState(WatchStreamState.RECONNECTING)
            } else if (s == ConnectionStatus.CONNECTED && stream.state.value == WatchStreamState.RECONNECTING) {
                // Refresh is handled by `authenticatedStream`, which fires after
                // the WS has re-issued all subscriptions (chart-history watch
                // re-registered before we refetch).
                stream.setState(WatchStreamState.CONNECTED)
            }
        }
    }

    jobs += scope.launch {
        ws.resumeStream.collect { refreshHistory() }
    }

    jobs += scope.launch {
        ws.authenticatedStream.collect { refreshHistory() }
    }

    jobs += scope.launch {
        val tick = maxOf(minOf(30L, mutex.withLock { resolutionSeconds }), 1L)
        while (isActive) {
            delay(tick * 1000)
            if (!isActive) return@launch
            val nowEpoch = nowEpochSeconds()
            var shouldRefresh = false
            mutex.withLock {
                val currentBoundary = bucketBoundary(nowEpoch, resolutionSeconds)
                val silenceMs = System.currentTimeMillis() - lastAggAtMs
                if (currentBoundary > hourBoundary &&
                    silenceMs > (BOUNDARY_AGG_SILENCE_FACTOR * resolutionSeconds * 1000).toLong()
                ) {
                    shouldRefresh = true
                }
            }
            if (shouldRefresh) refreshHistory()
        }
    }

    stream.stopAction = {
        ws.removeGapHandler(gapId)
        ws.unwatchChartHistory(chartWatchId)
        jobs.forEach { it.cancel() }
        aggStream.stop()
    }

    return stream
}

// MARK: - watchPnlChart

/**
 * Create a live P&L chart that merges historical data with real-time
 * aggregation updates and operation flows. The last point reflects current
 * live P&L; operation events update cumulative flows client-side.
 *
 * The SDK-appended live point always carries [ChartPointStatus.OPEN] so
 * consumers can identify it explicitly (e.g. to restyle, drop, or re-anchor
 * the live tip) instead of inferring it from its timestamp. Note its basis:
 * the live point derives from the live aggregation (equity including
 * unrealized P&L on open positions), while historical buckets come from the
 * server's history projection.
 *
 * The stream buffers the latest value and drops intermediate updates if the
 * consumer is slow. Updates are also dropped if the live point hasn't
 * materially changed. Call [PnlChartStream.stop] when done.
 *
 * @param path Object path or path prefix (trailing slash aggregates).
 * @param from Start timestamp (RFC 3339).
 * @param to End timestamp (RFC 3339). Live-tail behaviour mirrors
 *   [watchEquityChart].
 * @param points Number of historical samples (default 1000, max 1000).
 * @param exchange Exchange identifier for mid prices (default `"sim"`).
 * @param anchor [PnlAnchor.ZERO] (default) for standard P&L; [PnlAnchor.EQUITY]
 *   to also set each point's `valueUsd` to its account value (`equityUsd`), so
 *   the live value is the current account equity.
 *
 * Every `pnlUsd` the stream emits counts from one fixed point,
 * [PnlChartUpdate.baseline]: the first point of the opening history. When a
 * live window slides, refreshed history is re-derived onto that baseline, so
 * the line does not move.
 */
public suspend fun Arca.watchPnlChart(
    path: String,
    from: String,
    to: String,
    points: Int = 1000,
    exchange: String = "sim",
    anchor: PnlAnchor = PnlAnchor.ZERO,
): PnlChartStream {
    validatePath(path)

    val createdAtMs = System.currentTimeMillis()
    val fromInstant = parseInstantOrNull(from)
    val toInstant = parseInstantOrNull(to)
    val isLiveTail = fromInstant != null && toInstant != null &&
        abs(toInstant.toEpochMilli() - createdAtMs) <= LIVE_TAIL_THRESHOLD_S * 1000
    val windowSeconds: Long =
        if (isLiveTail) {
            // isLiveTail already implies fromInstant/toInstant are non-null
            // (see its definition above); the compiler smart-casts them here.
            maxOf(0L, toInstant.epochSecond - fromInstant.epochSecond)
        } else {
            0L
        }

    val mutex = Mutex()
    var windowFrom = from
    var windowTo = to

    fun slideIfLiveLocked() {
        if (!isLiveTail) return
        val now = Instant.now()
        windowFrom = iso8601String(now.minusSeconds(windowSeconds))
        windowTo = iso8601String(now)
    }

    val history = getPnlHistory(path, from, to, points)
    ws.watchPath(path)
    // `flowsSince` must name the instant `startingEquityUsd` is measured at,
    // or the live tick subtracts a flow already inside it. That is the close
    // of the first bucket, not the requested `from`.
    val flowsSince = alignedFlowsSince(history, fallback = from)
    val aggStream = watchAggregation(
        sources = listOf(AggregationSource(type = AggregationSourceType.PREFIX, value = path)),
        exchange = exchange,
        flowsSince = flowsSince,
    )

    var baseline: PnlChartBaseline? = pnlBaselineFrom(history.pnlPoints, history.startingEquityUsd)

    var cumInflows = 0.0
    var cumOutflows = 0.0
    for (flow in history.externalFlows ?: emptyList()) {
        val v = parseDoubleOrZero(flow.valueUsd)
        if (flow.direction == "inflow") cumInflows += v else cumOutflows += v
    }
    var flowInputs: List<String> = (history.externalFlows ?: emptyList()).map { it.valueUsd }

    var resolutionSeconds = chartResolutionSeconds(history.resolution)
    var hourBoundary = bucketBoundary(nowEpochSeconds(), resolutionSeconds)

    // Override the flow seed with server-provided cumulative flows from the
    // initial aggregation snapshot (authoritative, covers flowsSince..now).
    aggStream.aggregation.value?.let { agg ->
        parseDoubleOrNull(agg.cumInflowsUsd)?.let { cumInflows = it }
        parseDoubleOrNull(agg.cumOutflowsUsd)?.let { cumOutflows = it }
        if (agg.cumInflowsUsd != null || agg.cumOutflowsUsd != null) {
            flowInputs = listOfNotNull(agg.cumInflowsUsd, agg.cumOutflowsUsd)
        }
    }

    // Live P&L on the stream's baseline, at the precision of its inputs.
    fun livePnl(equity: String?): String {
        val start = baseline?.startingEquityUsd ?: history.startingEquityUsd
        val pnl = parseDoubleOrZero(equity) - parseDoubleOrZero(start) - cumInflows + cumOutflows
        return formatPnlUsd(pnl, listOf(equity, start) + flowInputs)
    }

    // Trim trailing points within the current time bucket so the server's live
    // tip doesn't create a discontinuity with the SDK's live (revalued) tip.
    val trimmed = rebasePnlPoints(history.pnlPoints, baseline).toMutableList()
    while (trimmed.isNotEmpty()) {
        val ts = parseInstantOrNull(trimmed.last().timestamp) ?: break
        if (ts.epochSecond > hourBoundary) trimmed.removeAt(trimmed.size - 1) else break
    }

    var historical = trimmed.toMutableList()
    var historyRevision = 0
    var lastEmittedRevision = 0
    var flows: List<ExternalFlowEntry> = history.externalFlows ?: emptyList()
    var liveEquity: String? = aggStream.aggregation.value?.totalEquityUsd
    var lastAggAtMs = System.currentTimeMillis()

    var initialChart = trimmed.toMutableList()
    aggStream.aggregation.value?.let { agg ->
        initialChart.add(
            PnlPoint(timestamp = iso8601String(Instant.now()), pnlUsd = livePnl(agg.totalEquityUsd), equityUsd = agg.totalEquityUsd, status = ChartPointStatus.OPEN),
        )
        if (anchor == PnlAnchor.EQUITY) initialChart = applyEquityAnchor(initialChart).toMutableList()
    }

    val stream = PnlChartStream()
    stream.setState(WatchStreamState.CONNECTED)
    stream.chartMut.value = initialChart
    stream.baselineMut.value = baseline

    // Installs a refetched window as the chart's history; call with `mutex`
    // held. A live window slides on every refresh, and the server counts the
    // new window from its own first point; re-derive it onto the baseline this
    // stream opened with, so the line never moves.
    fun absorbHistoryLocked(fresh: PnlHistoryResponse): List<PnlPoint> {
        resolutionSeconds = chartResolutionSeconds(fresh.resolution)
        hourBoundary = bucketBoundary(nowEpochSeconds(), resolutionSeconds)
        if (baseline == null) baseline = pnlBaselineFrom(fresh.pnlPoints, fresh.startingEquityUsd)
        val filtered = rebasePnlPoints(fresh.pnlPoints, baseline).filter { it.status != ChartPointStatus.OPEN }
        historical = filtered.toMutableList()
        historyRevision++
        flows = fresh.externalFlows ?: emptyList()
        return filtered
    }

    val chartWatchId = ws.watchChartHistory(path)
    val jobs = mutableListOf<Job>()

    suspend fun refreshHistory() {
        val w: Pair<String, String>
        mutex.withLock {
            slideIfLiveLocked()
            w = windowFrom to windowTo
        }
        val key = buildCacheKey(
            "pnlHistory",
            mapOf("target" to path, "kind" to "path", "from" to w.first, "to" to w.second, "points" to points.toString()),
        )
        historyCache.delete(key)
        val fresh = runCatching { getPnlHistory(path, w.first, w.second, points) }.getOrNull() ?: return
        val update: PnlChartUpdate
        mutex.withLock {
            var pts = absorbHistoryLocked(fresh).toMutableList()
            aggStream.aggregation.value?.let { agg ->
                pts.add(PnlPoint(timestamp = iso8601String(Instant.now()), pnlUsd = livePnl(agg.totalEquityUsd), equityUsd = agg.totalEquityUsd, status = ChartPointStatus.OPEN))
            }
            if (anchor == PnlAnchor.EQUITY) pts = applyEquityAnchor(pts).toMutableList()
            lastEmittedRevision = historyRevision
            update = PnlChartUpdate(points = pts, externalFlows = flows, baseline = baseline)
        }
        stream.push(update)
    }

    val gapId = ws.onGap {
        scope.launch {
            val w: Pair<String, String>
            mutex.withLock {
                slideIfLiveLocked()
                w = windowFrom to windowTo
            }
            val key = buildCacheKey(
                "pnlHistory",
                mapOf("target" to path, "kind" to "path", "from" to w.first, "to" to w.second, "points" to points.toString()),
            )
            historyCache.delete(key)
            val fresh = runCatching { getPnlHistory(path, w.first, w.second, points) }.getOrNull() ?: return@launch
            mutex.withLock { absorbHistoryLocked(fresh) }
        }
    }

    jobs += scope.launch {
        aggStream.updates.collect { agg ->
            var doRefresh = false
            var emit: PnlChartUpdate? = null
            mutex.withLock {
                lastAggAtMs = System.currentTimeMillis()
                // Capture the previous live values BEFORE absorbing the new agg
                // so a boundary cross emits the value current right before the
                // boundary (matching the TS PnlChartStream behaviour).
                val previousLiveEquityStr = liveEquity
                val previousLivePnl = livePnl(previousLiveEquityStr)

                val currentBoundary = bucketBoundary(nowEpochSeconds(), resolutionSeconds)
                val lastBoundary = hourBoundary

                if (currentBoundary - lastBoundary > resolutionSeconds && historical.isNotEmpty()) {
                    // Multi-bucket gap: refetch the dense server window which
                    // fills every intermediate bucket.
                    doRefresh = true
                } else {
                    if (currentBoundary > lastBoundary && previousLiveEquityStr != null && historical.isNotEmpty()) {
                        historical.add(
                            PnlPoint(
                                timestamp = iso8601String(Instant.ofEpochSecond(lastBoundary)),
                                pnlUsd = previousLivePnl,
                                equityUsd = previousLiveEquityStr,
                            ),
                        )
                        historyRevision++
                        hourBoundary = currentBoundary
                    }

                    parseDoubleOrNull(agg.cumInflowsUsd)?.let { cumInflows = it }
                    parseDoubleOrNull(agg.cumOutflowsUsd)?.let { cumOutflows = it }
                    if (agg.cumInflowsUsd != null || agg.cumOutflowsUsd != null) {
                        flowInputs = listOfNotNull(agg.cumInflowsUsd, agg.cumOutflowsUsd)
                    }
                    liveEquity = agg.totalEquityUsd

                    var allPoints = historical.toMutableList().apply {
                        add(PnlPoint(timestamp = iso8601String(Instant.now()), pnlUsd = livePnl(agg.totalEquityUsd), equityUsd = agg.totalEquityUsd, status = ChartPointStatus.OPEN))
                    }
                    if (anchor == PnlAnchor.EQUITY) allPoints = applyEquityAnchor(allPoints).toMutableList()

                    // Sub-cent moves update `chart` without waking consumers.
                    val prevPoints = stream.chartMut.value
                    val isSameValue = if (anchor == PnlAnchor.EQUITY) {
                        sameCents(prevPoints.lastOrNull()?.valueUsd, allPoints.lastOrNull()?.valueUsd)
                    } else {
                        sameCents(prevPoints.lastOrNull()?.pnlUsd, allPoints.lastOrNull()?.pnlUsd)
                    }

                    if (historyRevision == lastEmittedRevision && prevPoints.size == allPoints.size && isSameValue) {
                        stream.chartMut.value = allPoints
                    } else {
                        lastEmittedRevision = historyRevision
                        emit = PnlChartUpdate(points = allPoints, externalFlows = flows, baseline = baseline)
                    }
                }
            }
            if (doRefresh) refreshHistory() else emit?.let { stream.push(it) }
        }
    }

    jobs += scope.launch {
        ws.chartSnapshotEvents().collect { (eventWatchId, _) ->
            if (eventWatchId == chartWatchId) refreshHistory()
        }
    }

    jobs += scope.launch {
        ws.statusStream.collect { s ->
            if (s == ConnectionStatus.DISCONNECTED && stream.state.value != WatchStreamState.LOADING) {
                stream.setState(WatchStreamState.RECONNECTING)
            } else if (s == ConnectionStatus.CONNECTED && stream.state.value == WatchStreamState.RECONNECTING) {
                stream.setState(WatchStreamState.CONNECTED)
            }
        }
    }

    jobs += scope.launch {
        ws.resumeStream.collect { refreshHistory() }
    }

    jobs += scope.launch {
        ws.authenticatedStream.collect { refreshHistory() }
    }

    jobs += scope.launch {
        val tick = maxOf(minOf(30L, mutex.withLock { resolutionSeconds }), 1L)
        while (isActive) {
            delay(tick * 1000)
            if (!isActive) return@launch
            val nowEpoch = nowEpochSeconds()
            var shouldRefresh = false
            mutex.withLock {
                val currentBoundary = bucketBoundary(nowEpoch, resolutionSeconds)
                val silenceMs = System.currentTimeMillis() - lastAggAtMs
                if (currentBoundary > hourBoundary &&
                    silenceMs > (BOUNDARY_AGG_SILENCE_FACTOR * resolutionSeconds * 1000).toLong()
                ) {
                    shouldRefresh = true
                }
            }
            if (shouldRefresh) refreshHistory()
        }
    }

    stream.stopAction = {
        ws.removeGapHandler(gapId)
        ws.unwatchChartHistory(chartWatchId)
        ws.unwatchPath(path)
        jobs.forEach { it.cancel() }
        aggStream.stop()
    }

    return stream
}

// MARK: - Live convenience variants

/**
 * Convenience: open a live equity chart for a sliding range preset. Computes
 * `from`/`to` from the current wall clock and delegates to [watchEquityChart],
 * which slides the window forward on every refresh and self-heals across app
 * suspension.
 */
public suspend fun Arca.watchEquityChartLive(
    path: String,
    range: ChartRangePreset,
    points: Int = 1000,
    exchange: String = "sim",
): EquityChartStream {
    val (from, to) = Arca.computeChartRange(range)
    return watchEquityChart(path = path, from = from, to = to, points = points, exchange = exchange)
}

/**
 * Convenience: open a live P&L chart for a sliding range preset. See
 * [watchEquityChartLive].
 */
public suspend fun Arca.watchPnlChartLive(
    path: String,
    range: ChartRangePreset,
    points: Int = 1000,
    exchange: String = "sim",
    anchor: PnlAnchor = PnlAnchor.ZERO,
): PnlChartStream {
    val (from, to) = Arca.computeChartRange(range)
    return watchPnlChart(path = path, from = from, to = to, points = points, exchange = exchange, anchor = anchor)
}
