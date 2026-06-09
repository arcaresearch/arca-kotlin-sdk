package network.arca.sdk

import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import network.arca.sdk.models.ActiveAssetData
import network.arca.sdk.models.CandleEvent
import network.arca.sdk.models.CandleInterval
import network.arca.sdk.models.ConnectionStatus
import network.arca.sdk.models.EventEnvelope
import network.arca.sdk.models.ExchangeState
import network.arca.sdk.models.Fill
import network.arca.sdk.models.MarginTier
import network.arca.sdk.models.ObjectValuation
import network.arca.sdk.models.RealmEvent
import network.arca.sdk.models.revalued
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

private fun hasInlineStructuralExchangeState(state: ExchangeState?): Boolean = state?.pendingIntents != null

// MARK: - watchOperations

/**
 * Watch real-time operation events under a path prefix. The server sends initial
 * operations in the snapshot, then streams `operation.created` /
 * `operation.updated`. Reconnections and gap recovery are handled automatically.
 * Call [OperationWatchStream.stop] when done.
 */
public suspend fun Arca.watchOperations(path: String = "/"): OperationWatchStream {
    ws.ensureConnected()
    val stream = OperationWatchStream()
    val jobs = mutableListOf<Job>()

    jobs += scope.launch {
        ws.statusStream.collect { s ->
            if (s == ConnectionStatus.DISCONNECTED && stream.state.value != WatchStreamState.LOADING) {
                stream.setState(WatchStreamState.RECONNECTING)
            }
        }
    }

    suspend fun refetch() {
        try {
            val resp = if (path != "/") listOperations(path = path) else listOperations()
            stream.operationsMut.value = resp.operations
        } catch (e: Throwable) {
            log.warning("watch", e, mapOf("path" to path)) { "operations refetch failed" }
        }
    }

    val gapId = ws.onGap { scope.launch { refetch() } }

    ws.watchPath(path)
    refetch()
    stream.setState(WatchStreamState.CONNECTED)

    jobs += scope.launch {
        ws.operationEvents().collect { (op, event) ->
            val cur = stream.operationsMut.value
            val idx = cur.indexOfFirst { it.id == op.id }
            stream.operationsMut.value = if (idx >= 0) {
                cur.toMutableList().also { it[idx] = op }
            } else {
                buildList { add(op); addAll(cur) }
            }
            stream.push(op, event)
        }
    }

    stream.stopAction = {
        jobs.forEach { it.cancel() }
        ws.removeGapHandler(gapId)
        ws.unwatchPath(path)
    }
    stream.ready()
    return stream
}

// MARK: - watchBalances

/**
 * Watch real-time balance updates under a path prefix. The server sends initial
 * balances in the snapshot, then streams `balance.updated`. Reconnections and
 * gap recovery are handled automatically. Call [BalanceWatchStream.stop] when done.
 */
public suspend fun Arca.watchBalances(path: String = "/"): BalanceWatchStream {
    ws.ensureConnected()
    val stream = BalanceWatchStream()
    val jobs = mutableListOf<Job>()

    jobs += scope.launch {
        ws.statusStream.collect { s ->
            if (s == ConnectionStatus.DISCONNECTED && stream.state.value != WatchStreamState.LOADING) {
                stream.setState(WatchStreamState.RECONNECTING)
            }
        }
    }

    val gapId = ws.onGap {
        scope.launch {
            val entities = stream.balancesMut.value
            for ((entityId, snap) in entities) {
                try {
                    val bals = getBalances(entityId)
                    stream.balancesMut.update { it + (entityId to BalanceSnapshot(entityId, snap.entityPath, bals)) }
                } catch (e: Throwable) {
                    log.warning("watch", e, mapOf("entityId" to entityId)) { "balances gap recovery refetch failed" }
                }
            }
        }
    }

    ws.watchPath(path)

    try {
        val objects = listObjects(path = if (path == "/") null else path)
        for (obj in objects.objects) {
            try {
                val bals = getBalances(obj.id.value)
                if (bals.isNotEmpty()) {
                    stream.balancesMut.update {
                        it + (obj.id.value to BalanceSnapshot(obj.id.value, obj.path, bals))
                    }
                }
            } catch (e: Throwable) {
                log.warning("watch", e, mapOf("entityId" to obj.id.value, "path" to obj.path)) {
                    "balances initial snapshot failed for object"
                }
            }
        }
    } catch (e: Throwable) {
        log.warning("watch", e, mapOf("path" to path)) { "balances initial listObjects failed" }
    }
    stream.setState(WatchStreamState.CONNECTED)

    jobs += scope.launch {
        ws.balanceEvents().collect { (entityId, event) ->
            val eventPath = event.entityPath
            if (path != "/" && eventPath != null && !eventPath.startsWith(path)) return@collect
            stream.push(entityId, event)
        }
    }

    stream.stopAction = {
        jobs.forEach { it.cancel() }
        ws.removeGapHandler(gapId)
        ws.unwatchPath(path)
    }
    stream.ready()
    return stream
}

// MARK: - watchObject

/**
 * Watch real-time valuation updates for a single Arca object. The server streams
 * `object.valuation` events on structural changes; mid-price revaluation is
 * performed client-side so valuations update in real time without consuming
 * server bandwidth on every tick. Call [ObjectWatchStream.stop] when done.
 */
public suspend fun Arca.watchObject(path: String, exchange: String = "sim"): ObjectWatchStream {
    ws.ensureConnected()
    val stream = ObjectWatchStream(path)
    val mids = MutableStateFlow<Map<String, String>>(emptyMap())
    val stopped = AtomicBoolean(false)
    val jobs = mutableListOf<Job>()

    jobs += scope.launch {
        ws.statusStream.collect { s ->
            if (s == ConnectionStatus.DISCONNECTED && stream.state.value != WatchStreamState.LOADING) {
                stream.setState(WatchStreamState.RECONNECTING)
            } else if (s == ConnectionStatus.CONNECTED && stream.watchId.value != null) {
                stream.setState(WatchStreamState.CONNECTED)
            }
        }
    }

    val gapId = ws.onGap {
        scope.launch {
            if (stopped.get()) return@launch
            val v = try {
                getObjectValuation(path)
            } catch (e: Throwable) {
                log.warning("watch", e, mapOf("path" to path)) { "object valuation gap recovery refetch failed" }
                return@launch
            }
            if (stopped.get()) return@launch
            val cur = mids.value
            stream.push(if (cur.isEmpty()) v else v.revalued(cur))
        }
    }

    ws.acquireMids(exchange)

    jobs += scope.launch {
        ws.objectValuationEvents().collect { evt ->
            if (evt.path != path) return@collect
            stream.watchIdMut.value = evt.watchId
            if (evt.event.driftCorrected == true) {
                log.warning("watch", metadata = mapOf("path" to evt.path, "watchId" to evt.watchId)) {
                    "valuation drift corrected; previous value was stale"
                }
            }
            val cur = mids.value
            stream.setState(WatchStreamState.CONNECTED)
            stream.push(if (cur.isEmpty()) evt.valuation else evt.valuation.revalued(cur))
        }
    }

    jobs += scope.launch {
        ws.midsEvents().collect { m ->
            mids.update { it + m }
            val base = stream.valuationMut.value ?: return@collect
            stream.push(base.revalued(mids.value))
        }
    }

    ws.watchPath(path)

    stream.stopAction = {
        stopped.set(true)
        jobs.forEach { it.cancel() }
        ws.removeGapHandler(gapId)
        ws.releaseMids()
        ws.unwatchPath(path)
    }
    return stream
}

// MARK: - watchObjects

/**
 * Watch real-time valuations for multiple Arca objects. Creates one
 * [ObjectWatchStream] per path and merges updates into a map keyed by object
 * path. Duplicate paths are ignored (first wins). Call [ObjectsWatchStream.stop]
 * when done.
 */
public suspend fun Arca.watchObjects(paths: List<String>, exchange: String = "sim"): ObjectsWatchStream {
    val seen = LinkedHashSet<String>()
    val uniquePaths = paths.filter { seen.add(it) }

    if (uniquePaths.isEmpty()) {
        val empty = ObjectsWatchStream(emptyList())
        empty.setState(WatchStreamState.CONNECTED)
        empty.stopAction = {}
        return empty
    }

    val childStreams = uniquePaths.map { watchObject(it, exchange) }
    val stream = ObjectsWatchStream(childStreams)
    val lock = ReentrantLock()
    val stopped = AtomicBoolean(false)
    val unsubs = mutableListOf<() -> Unit>()
    val jobs = mutableListOf<Job>()

    fun refreshMergedState() {
        val states = childStreams.map { it.state.value }
        stream.setState(
            when {
                states.contains(WatchStreamState.RECONNECTING) -> WatchStreamState.RECONNECTING
                states.contains(WatchStreamState.LOADING) -> WatchStreamState.LOADING
                else -> WatchStreamState.CONNECTED
            },
        )
    }

    fun emit() {
        refreshMergedState()
        stream.push(stream.valuations.value)
    }

    lock.withLock {
        for (child in childStreams) {
            val childPath = child.path
            child.valuation.value?.let { v ->
                stream.valuationsMut.update { it + (childPath to v) }
            }
            unsubs += child.onUpdate { v ->
                stream.valuationsMut.update { it + (childPath to v) }
                emit()
            }
            jobs += scope.launch { child.state.collect { refreshMergedState() } }
        }
    }
    emit()

    stream.stopAction = {
        if (stopped.compareAndSet(false, true)) {
            lock.withLock {
                unsubs.forEach { it() }
                unsubs.clear()
            }
            jobs.forEach { it.cancel() }
            childStreams.forEach { it.stop() }
        }
    }
    return stream
}

// MARK: - watchAggregation

/**
 * Watch real-time aggregation updates for a set of sources. Creates a standalone
 * aggregation watch (not path-scoped); handles structural change events and
 * client-side revaluation from mid prices. Call [AggregationWatchStream.stop]
 * when done.
 */
public suspend fun Arca.watchAggregation(
    sources: List<network.arca.sdk.models.AggregationSource>,
    exchange: String = "sim",
    flowsSince: String? = null,
): AggregationWatchStream {
    ws.ensureConnected()

    val watchResponse = createAggregationWatch(sources, flowsSince)
    val stream = AggregationWatchStream(watchResponse.watchId.value)
    val mids = MutableStateFlow<Map<String, String>>(emptyMap())
    val structural = MutableStateFlow(watchResponse.aggregation)
    val widBox = MutableStateFlow(watchResponse.watchId.value)
    val stopped = AtomicBoolean(false)
    val refreshing = AtomicBoolean(false)
    val jobs = mutableListOf<Job>()

    stream.aggregationMut.value = watchResponse.aggregation

    jobs += scope.launch {
        ws.statusStream.collect { s ->
            if (s == ConnectionStatus.DISCONNECTED && stream.state.value != WatchStreamState.LOADING) {
                stream.setState(WatchStreamState.RECONNECTING)
            } else if (s == ConnectionStatus.CONNECTED && stream.state.value == WatchStreamState.RECONNECTING) {
                if (stopped.get() || !refreshing.compareAndSet(false, true)) return@collect
                try {
                    val oldWatchId = widBox.value
                    val newWatch = createAggregationWatch(sources, flowsSince)
                    if (stopped.get()) {
                        refreshing.set(false)
                        return@collect
                    }
                    widBox.value = newWatch.watchId.value
                    try {
                        destroyAggregationWatch(oldWatchId)
                    } catch (e: Throwable) {
                        log.debug("watch", e, mapOf("watchId" to oldWatchId)) {
                            "destroyAggregationWatch cleanup failed (best-effort)"
                        }
                    }
                    structural.value = newWatch.aggregation
                    val cur = mids.value
                    stream.push(if (cur.isEmpty()) newWatch.aggregation else newWatch.aggregation.revalued(cur))
                } catch (_: Throwable) {
                    // Best effort — keep existing data
                }
                refreshing.set(false)
                stream.setState(WatchStreamState.CONNECTED)
            }
        }
    }

    ws.acquireMids(exchange)

    jobs += scope.launch {
        ws.aggregationEvents().collect { (eventWatchId, agg, _) ->
            if (eventWatchId != widBox.value || agg == null) return@collect
            structural.value = agg
            val cur = mids.value
            stream.setState(WatchStreamState.CONNECTED)
            stream.push(if (cur.isEmpty()) agg else agg.revalued(cur))
        }
    }

    jobs += scope.launch {
        ws.midsEvents().collect { m ->
            mids.update { it + m }
            val base = structural.value ?: return@collect
            stream.push(base.revalued(mids.value))
        }
    }

    stream.setState(WatchStreamState.CONNECTED)

    stream.stopAction = {
        stopped.set(true)
        jobs.forEach { it.cancel() }
        ws.releaseMids()
        scope.launch {
            try {
                destroyAggregationWatch(widBox.value)
            } catch (e: Throwable) {
                log.debug("watch", e, mapOf("watchId" to widBox.value)) {
                    "destroyAggregationWatch cleanup failed (best-effort)"
                }
            }
        }
    }
    return stream
}

// MARK: - watchExchangeState

/**
 * Watch real-time exchange state for an Arca exchange object. Fetches initial
 * state via REST, then re-fetches on each `exchange.updated` event matching the
 * object (or applies inline structural state when present) and revalues from mid
 * prices. Call [ExchangeStateWatchStream.stop] when done.
 */
public suspend fun Arca.watchExchangeState(objectId: String, exchange: String = "sim"): ExchangeStateWatchStream {
    ws.ensureConnected()

    val detail = getObjectDetail(objectId)
    val objectPath = detail.`object`.path

    val stream = ExchangeStateWatchStream()
    val structural = MutableStateFlow<ExchangeState?>(null)
    val mids = MutableStateFlow<Map<String, String>>(emptyMap())
    val jobs = mutableListOf<Job>()

    val initial = getExchangeState(objectId)
    structural.value = initial
    stream.exchangeStateMut.value = initial
    stream.setState(WatchStreamState.CONNECTED)

    jobs += scope.launch {
        ws.statusStream.collect { s ->
            if (s == ConnectionStatus.DISCONNECTED && stream.state.value != WatchStreamState.LOADING) {
                stream.setState(WatchStreamState.RECONNECTING)
            } else if (s == ConnectionStatus.CONNECTED && stream.state.value == WatchStreamState.RECONNECTING) {
                try {
                    val refreshed = getExchangeState(objectId)
                    structural.value = refreshed
                    val cur = mids.value
                    stream.exchangeStateMut.value = if (cur.isEmpty()) refreshed else refreshed.revalued(cur)
                } catch (e: Throwable) {
                    log.warning("watch", e, mapOf("objectId" to objectId)) {
                        "exchange state refresh on reconnect failed"
                    }
                }
                stream.setState(WatchStreamState.CONNECTED)
            }
        }
    }

    ws.acquireMids(exchange)
    ws.watchPath(objectPath)

    jobs += scope.launch {
        ws.exchangeNotifications().collect { event ->
            if (event.entityId != objectId && event.entityPath != objectPath) return@collect
            val structuralState: ExchangeState = run {
                val inline = event.exchangeState
                if (inline != null && hasInlineStructuralExchangeState(inline)) {
                    inline
                } else {
                    try {
                        getExchangeState(objectId)
                    } catch (e: Throwable) {
                        log.warning("watch", e, mapOf("objectId" to objectId)) { "exchange state refetch failed" }
                        return@collect
                    }
                }
            }
            structural.value = structuralState
            val cur = mids.value
            stream.push(if (cur.isEmpty()) structuralState else structuralState.revalued(cur))
        }
    }

    jobs += scope.launch {
        ws.midsEvents().collect { m ->
            mids.update { it + m }
            val base = structural.value ?: return@collect
            stream.push(base.revalued(mids.value))
        }
    }

    stream.stopAction = {
        jobs.forEach { it.cancel() }
        ws.unwatchPath(objectPath)
        ws.releaseMids()
    }
    return stream
}

// MARK: - watchFunding

/**
 * Watch real-time funding payment events for an exchange Arca object. Yields each
 * funding payment with its [EventEnvelope] for correlation. Call
 * [FundingWatchStream.stop] when done.
 */
public suspend fun Arca.watchFunding(objectId: String): FundingWatchStream {
    ws.ensureConnected()

    val detail = getObjectDetail(objectId)
    val objectPath = detail.`object`.path

    val stream = FundingWatchStream()
    stream.setState(WatchStreamState.CONNECTED)
    val jobs = mutableListOf<Job>()

    jobs += scope.launch {
        ws.statusStream.collect { s ->
            when (s) {
                ConnectionStatus.DISCONNECTED -> stream.setState(WatchStreamState.RECONNECTING)
                ConnectionStatus.CONNECTED -> stream.setState(WatchStreamState.CONNECTED)
                else -> {}
            }
        }
    }

    ws.watchPath(objectPath)

    jobs += scope.launch {
        ws.fundingEvents().collect { (payment, event) ->
            if (event.entityId != objectId) return@collect
            stream.push(payment, EventEnvelope.from(event))
        }
    }

    stream.stopAction = {
        jobs.forEach { it.cancel() }
        ws.unwatchPath(objectPath)
    }
    return stream
}

// MARK: - watchFills

/**
 * Watch fills (trade history) for an exchange Arca object. Two-phase delivery:
 * `fill.previewed` (instant preview, matched by `correlationId`) is replaced by
 * the authoritative `fill.recorded`. A convergence timeout fires if a preview
 * does not receive its authoritative update within the window. On reconnect or
 * gap, re-fetches from REST to reconcile. Call [FillWatchStream.stop] when done.
 *
 * Read [FillWatchStream.fills] for the merged activity-feed view (one row per
 * fill); read [FillWatchStream.updates] only when you need the
 * preview→recorded transition itself.
 */
public suspend fun Arca.watchFills(
    objectId: String,
    market: String? = null,
    limit: Int? = null,
): FillWatchStream {
    ws.ensureConnected()

    val stream = FillWatchStream()
    val lock = ReentrantLock()
    val fillIdSet = HashSet<String>()
    val previewCorrelations = HashMap<String, Job>()
    val resolvedCorrelations = HashSet<String>()
    val fetchInFlight = AtomicBoolean(false)
    val jobs = mutableListOf<Job>()

    val detail = getObjectDetail(objectId)
    val objectPath = detail.`object`.path

    fun matchesObject(event: RealmEvent): Boolean =
        event.entityId == objectId || event.entityPath == objectPath

    fun clearAllTimers() {
        lock.withLock {
            previewCorrelations.values.forEach { it.cancel() }
            previewCorrelations.clear()
        }
    }

    suspend fun fetchFills() {
        if (!fetchInFlight.compareAndSet(false, true)) return
        try {
            val resp = try {
                listFills(objectId = objectId, market = market, limit = limit)
            } catch (e: Throwable) {
                log.warning("watch", e, mapOf("objectId" to objectId, "market" to (market ?: ""))) {
                    "fills snapshot refetch failed"
                }
                return
            }
            lock.withLock {
                stream.fillsMut.value = resp.fills
                fillIdSet.clear()
                resp.fills.forEach { fillIdSet.add(it.id) }
                previewCorrelations.values.forEach { it.cancel() }
                previewCorrelations.clear()
                resolvedCorrelations.clear()
            }
            stream.setState(WatchStreamState.CONNECTED)
        } finally {
            fetchInFlight.set(false)
        }
    }

    jobs += scope.launch {
        ws.statusStream.collect { s ->
            if (s == ConnectionStatus.DISCONNECTED && stream.state.value != WatchStreamState.LOADING) {
                stream.setState(WatchStreamState.RECONNECTING)
            } else if (s == ConnectionStatus.CONNECTED && stream.fills.value.isNotEmpty()) {
                fetchFills()
            }
        }
    }

    val gapId = ws.onGap { scope.launch { fetchFills() } }

    ws.watchPath(objectPath)

    jobs += scope.launch {
        ws.fillEvents().collect { (simFill, event) ->
            if (!matchesObject(event)) return@collect
            val orderId = simFill.orderId.value
            val correlationKey = event.correlationId ?: orderId

            val skip = lock.withLock {
                previewCorrelations.containsKey(correlationKey) || resolvedCorrelations.contains(correlationKey)
            }
            if (skip) return@collect

            val preview = Fill(
                id = simFill.id.value,
                orderId = orderId,
                market = simFill.market,
                side = simFill.side,
                size = simFill.size,
                price = simFill.price,
                fee = simFill.fee,
                builderFee = simFill.builderFee,
                realizedPnl = simFill.realizedPnl,
                isLiquidation = simFill.isLiquidation,
                createdAt = simFill.createdAt,
            )

            val timerJob = scope.launch {
                delay(FillWatchStream.CONVERGENCE_TIMEOUT_MS)
                val stillPending = lock.withLock { previewCorrelations.containsKey(correlationKey) }
                if (stillPending) stream.fireConvergenceTimeout(correlationKey)
            }

            lock.withLock {
                previewCorrelations[correlationKey] = timerJob
                stream.fillsMut.value = buildList { add(preview); addAll(stream.fillsMut.value) }
            }
            stream.push(preview, event)
        }
    }

    jobs += scope.launch {
        ws.fillRecordedEvents().collect { (fill, event) ->
            if (!matchesObject(event)) return@collect
            val correlationKey = event.correlationId ?: fill.orderId

            lock.withLock {
                var replaced = false
                if (correlationKey != null) {
                    val hadPreview = previewCorrelations.containsKey(correlationKey)
                    val cur = stream.fillsMut.value.toMutableList()
                    val idx = if (hadPreview) {
                        cur.indexOfFirst {
                            (it.orderId == correlationKey || it.orderId == fill.orderId) && it.operationId == null
                        }
                    } else {
                        cur.indexOfFirst { it.orderId == correlationKey && it.operationId == null }
                    }
                    if (idx >= 0) {
                        cur[idx] = fill
                        stream.fillsMut.value = cur
                        replaced = true
                    }
                    previewCorrelations.remove(correlationKey)?.cancel()
                    resolvedCorrelations.add(correlationKey)
                }
                if (!replaced) {
                    if (fillIdSet.contains(fill.id)) return@withLock
                    stream.fillsMut.value = buildList { add(fill); addAll(stream.fillsMut.value) }
                }
                fillIdSet.add(fill.id)
            }
            stream.push(fill, event)
        }
    }

    fetchFills()

    stream.stopAction = {
        jobs.forEach { it.cancel() }
        clearAllTimers()
        ws.removeGapHandler(gapId)
        ws.unwatchPath(objectPath)
    }
    return stream
}

// MARK: - watchCandles

/**
 * Subscribe to raw real-time candle events (no history blending). For
 * candlestick charts prefer [watchCandleChart], which loads history, merges live
 * events, and handles reconnection gaps. Call [CandleWatchStream.stop] when done.
 */
public suspend fun Arca.watchCandles(coins: List<String>, intervals: List<CandleInterval>): CandleWatchStream {
    ws.ensureConnected()

    val stream = CandleWatchStream()
    stream.setState(WatchStreamState.CONNECTED)
    val coinSet = coins.toSet()
    val jobs = mutableListOf<Job>()

    jobs += scope.launch {
        ws.statusStream.collect { s ->
            when (s) {
                ConnectionStatus.DISCONNECTED -> stream.setState(WatchStreamState.RECONNECTING)
                ConnectionStatus.CONNECTED -> stream.setState(WatchStreamState.CONNECTED)
                else -> {}
            }
        }
    }

    ws.acquireCandles(coins, intervals)

    jobs += scope.launch {
        ws.candleEvents().collect { event: CandleEvent ->
            if (coinSet.isEmpty() || coinSet.contains(event.market)) {
                stream.push(event)
            }
        }
    }

    stream.stopAction = {
        jobs.forEach { it.cancel() }
        ws.releaseCandles(coins, intervals)
    }
    return stream
}

// MARK: - watchPrices

/**
 * Watch real-time mid prices. Each update is a full snapshot of all known prices.
 * Call [MarketPriceStream.stop] when done.
 */
public suspend fun Arca.watchPrices(exchange: String = "sim"): MarketPriceStream {
    ws.ensureConnected()

    val stream = MarketPriceStream()
    val jobs = mutableListOf<Job>()

    jobs += scope.launch {
        ws.statusStream.collect { s ->
            if (s == ConnectionStatus.DISCONNECTED && stream.state.value != WatchStreamState.LOADING) {
                stream.setState(WatchStreamState.RECONNECTING)
            }
        }
    }

    ws.acquireMids(exchange)

    jobs += scope.launch {
        ws.midsEvents().collect { mids ->
            val cur = stream.prices.value
            val merged = cur + mids
            stream.setState(WatchStreamState.CONNECTED)
            if (merged != cur) stream.push(merged)
        }
    }

    stream.stopAction = {
        jobs.forEach { it.cancel() }
        ws.releaseMids()
    }
    stream.ready()
    return stream
}

// MARK: - watchMaxOrderSize

/**
 * Subscribe to a live, SDK-derived max order size stream for a coin/side. Uses
 * [getExchangeState] + [watchPrices] and recomputes on price or exchange-state
 * changes. When the object is server-priced, max-order-size comes from the
 * server's active-asset-data endpoint instead of local derivation. Call
 * [MaxOrderSizeWatchStream.stop] when done.
 */
public suspend fun Arca.watchMaxOrderSize(options: MaxOrderSizeWatchOptions): MaxOrderSizeWatchStream {
    ws.ensureConnected()

    val stream = MaxOrderSizeWatchStream()
    val priceStream = watchPrices()

    val initialExchangeState: ExchangeState = try {
        getExchangeState(options.objectId)
    } catch (e: Throwable) {
        priceStream.stop()
        throw e
    }

    var resolvedFeeScale = options.feeScale ?: 1.0
    if (options.feeScale == null) {
        val scale = runCatching { market(options.market) }.getOrNull()?.feeScale
        if (scale != null && scale > 0) resolvedFeeScale = scale
    }

    var mmr: String? = options.maintenanceMarginRate
    var tiers: List<MarginTier>? = null
    var askRatio = 1.0
    var bidRatio = 1.0
    runCatching {
        getActiveAssetData(
            objectId = options.objectId,
            market = options.market,
            applicationFeeTenthsBps = options.builderFeeBps,
            leverage = options.leverage,
        )
    }.getOrNull()?.let { data ->
        if (options.maintenanceMarginRate == null) mmr = data.maintenanceMarginRate
        data.marginTiers?.takeIf { it.isNotEmpty() }?.let { tiers = it }
        val mid = data.markPx.toDoubleOrNull()
        if (mid != null && mid > 0) {
            data.bidPx?.toDoubleOrNull()?.let { if (it > 0) bidRatio = it / mid }
            data.askPx?.toDoubleOrNull()?.let { if (it > 0) askRatio = it / mid }
        }
    }

    val exchangeStateBox = MutableStateFlow<ExchangeState?>(initialExchangeState)
    val jobs = mutableListOf<Job>()

    fun recompute(): ActiveAssetData? {
        val exState = exchangeStateBox.value ?: return null
        val markPx = priceStream.prices.value[options.market]?.toDoubleOrNull() ?: 0.0
        return deriveActiveAssetData(
            exchangeState = exState,
            market = options.market,
            markPx = markPx,
            leverage = options.leverage,
            side = options.side,
            builderFeeBps = options.builderFeeBps,
            szDecimals = options.szDecimals,
            feeScale = resolvedFeeScale,
            maintenanceMarginRate = mmr,
            marginTiers = tiers,
            askRatio = askRatio,
            bidRatio = bidRatio,
        )
    }

    suspend fun fetchServerActiveAssetData(): ActiveAssetData? = runCatching {
        getActiveAssetData(
            objectId = options.objectId,
            market = options.market,
            applicationFeeTenthsBps = options.builderFeeBps,
            leverage = options.leverage,
        )
    }.getOrNull()

    if (initialExchangeState.pricingMode == network.arca.sdk.models.PricingMode.SERVER) {
        fetchServerActiveAssetData()?.let { stream.activeAssetDataMut.value = it }
    } else {
        recompute()?.let { stream.activeAssetDataMut.value = it }
    }

    val detail = getObjectDetail(options.objectId)
    val objectPath = detail.`object`.path
    ws.watchPath(objectPath)

    jobs += scope.launch {
        ws.statusStream.collect { s ->
            if (s == ConnectionStatus.DISCONNECTED && stream.state.value != WatchStreamState.LOADING) {
                stream.setState(WatchStreamState.RECONNECTING)
            }
        }
    }

    jobs += scope.launch {
        ws.exchangeNotifications().collect { event ->
            if (event.entityId != options.objectId && event.entityPath != objectPath) return@collect
            val nextState = event.exchangeState ?: run {
                runCatching { getExchangeState(options.objectId) }.getOrNull() ?: return@collect
            }
            exchangeStateBox.value = nextState
            val data = if (nextState.pricingMode == network.arca.sdk.models.PricingMode.SERVER) {
                fetchServerActiveAssetData()
            } else {
                recompute()
            }
            if (data != null) {
                stream.push(data)
                stream.setState(WatchStreamState.CONNECTED)
            }
        }
    }

    jobs += scope.launch {
        priceStream.updates.collect {
            if (exchangeStateBox.value?.pricingMode == network.arca.sdk.models.PricingMode.SERVER) return@collect
            recompute()?.let {
                stream.push(it)
                stream.setState(WatchStreamState.CONNECTED)
            }
        }
    }

    if (stream.activeAssetData.value != null) stream.setState(WatchStreamState.CONNECTED)

    stream.stopAction = {
        jobs.forEach { it.cancel() }
        priceStream.stop()
        ws.unwatchPath(objectPath)
    }
    return stream
}
