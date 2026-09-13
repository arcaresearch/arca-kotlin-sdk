package network.arca.sdk

import kotlinx.coroutines.Job
import kotlinx.coroutines.ensureActive
import kotlin.coroutines.coroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import network.arca.sdk.models.ActiveAssetData
import network.arca.sdk.models.CandleEvent
import network.arca.sdk.models.CandleInterval
import network.arca.sdk.models.OIEvent
import network.arca.sdk.models.ConnectionStatus
import network.arca.sdk.models.EventEnvelope
import network.arca.sdk.models.ExchangeState
import network.arca.sdk.models.observedBefore
import network.arca.sdk.models.Fill
import network.arca.sdk.models.MarginTier
import network.arca.sdk.models.ObjectValuation
import network.arca.sdk.models.RealmEvent
import network.arca.sdk.models.revalued
import java.time.Instant
import java.time.Duration
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
        // Mirror Swift: yield one empty snapshot so `updates` consumers resolve.
        empty.push(emptyMap())
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

    // The watch lives on the server side of a specific connection, so whenever
    // that connection is replaced it has to be re-created against the new one.
    // Returns false when the caller should leave stream state alone.
    suspend fun recreateWatch(): Boolean {
        if (stopped.get() || !refreshing.compareAndSet(false, true)) return false
        try {
            val oldWatchId = widBox.value
            val newWatch = createAggregationWatch(sources, flowsSince)
            if (stopped.get()) return false
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
        } finally {
            refreshing.set(false)
        }
        return true
    }

    jobs += scope.launch {
        ws.statusStream.collect { s ->
            if (s == ConnectionStatus.DISCONNECTED && stream.state.value != WatchStreamState.LOADING) {
                stream.setState(WatchStreamState.RECONNECTING)
            } else if (s == ConnectionStatus.CONNECTED && stream.state.value == WatchStreamState.RECONNECTING) {
                if (!recreateWatch()) return@collect
                stream.setState(WatchStreamState.CONNECTED)
            }
        }
    }

    // A rotation swaps the socket without an outage, so no status change fires
    // and the branch above never runs — but the watch still died with the
    // connection that retired, so without this the stream goes permanently quiet
    // with no error. State stays CONNECTED: nothing was missed.
    jobs += scope.launch {
        ws.rotatedStream.collect { recreateWatch() }
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
    val visible = positionView(objectId)
    val structural = MutableStateFlow<ExchangeState?>(null)
    val mids = MutableStateFlow<Map<String, String>>(emptyMap())
    val jobs = mutableListOf<Job>()
    val observationLock = Any()
    var observationEpoch = 0L
    var expiryJob: Job? = null
    var recoveryJob: Job? = null
    var recoveryAttempt = 0
    val stopped = java.util.concurrent.atomic.AtomicBoolean(false)
    // Coalescing gate for REST re-reads: a request while one is in flight runs
    // once more after it lands. The in-flight read belongs to an older epoch
    // and would otherwise be the only read of that burst.
    var refetchInFlight = false
    var refetchQueued = false
    val gateLock = Any()
    lateinit var scheduleRecovery: () -> Unit

    fun clearRecovery() {
        recoveryJob?.cancel()
        recoveryJob = null
        recoveryAttempt = 0
    }

    /** Drop the current observation and arm the fallback re-read. Callers hold [observationLock]. */
    fun invalidate() {
        expiryJob?.cancel()
        expiryJob = null
        visible.invalidate()
        structural.value = null
        stream.exchangeStateMut.value = null
        stream.setState(WatchStreamState.RECONNECTING)
        scheduleRecovery()
    }

    fun armExpiry(state: ExchangeState, epoch: Long): Boolean {
        expiryJob?.cancel()
        expiryJob = null
        val allocation = state.tradingAllocation ?: return true
        val rawDeadline = allocation.validUntil ?: return true
        val remaining = runCatching {
            val deadline = Instant.parse(rawDeadline)
            val asOf = allocation.asOf?.let(Instant::parse) ?: Instant.now()
            minOf(Duration.between(Instant.now(), deadline).toMillis(), Duration.between(asOf, deadline).toMillis())
        }.getOrDefault(0)
        if (remaining <= 0) {
            invalidate()
            return false
        }
        expiryJob = scope.launch {
            delay(minOf(remaining, 86_400_000))
            synchronized(observationLock) {
                if (observationEpoch == epoch) {
                    observationEpoch++
                    invalidate()
                }
            }
        }
        return true
    }

    /**
     * Apply a structural state observed at [epoch], unless a newer observation
     * has landed since. The single write path for pushes, re-reads and the
     * periodic refresh.
     */
    fun applyObservation(state: ExchangeState, epoch: Long) {
        synchronized(observationLock) {
            // The epoch orders this read against pushes that arrived while it
            // was in flight; the read time orders it against the state already
            // applied. A frame the platform read before that state describes an
            // older ledger — a pre-fill snapshot resolving late — and must not
            // replace it.
            val applied = structural.value
            if (applied != null && state.observedBefore(applied)) {
                log.debug("watch", metadata = mapOf("objectId" to objectId, "observedAt" to (state.observedAt ?: ""), "appliedAt" to (applied.observedAt ?: ""))) {
                    "dropped an exchange state observed before the one applied"
                }
                return
            }
            if (epoch == observationEpoch && armExpiry(state, ++observationEpoch)) {
                clearRecovery()
                structural.value = state
                val cur = mids.value
                stream.setState(WatchStreamState.CONNECTED)
                val value = if (cur.isEmpty()) state else state.revalued(cur)
                stream.push(value)
                visible.observe(value)
            }
        }
    }

    /** Coalesced, epoch-guarded REST re-read. */
    suspend fun refetch() {
        val run = synchronized(gateLock) {
            if (refetchInFlight) { refetchQueued = true; false } else { refetchInFlight = true; true }
        }
        if (!run) return
        while (true) {
            if (stopped.get()) break
            val epoch = synchronized(observationLock) { observationEpoch }
            try {
                val fresh = getExchangeState(objectId)
                if (!stopped.get()) applyObservation(fresh, epoch)
            } catch (cancelled: kotlinx.coroutines.CancellationException) {
                throw cancelled
            } catch (e: Throwable) {
                log.warning("watch", e, mapOf("objectId" to objectId)) { "exchange state refetch failed" }
            }
            val again = synchronized(gateLock) {
                if (refetchQueued && !stopped.get()) { refetchQueued = false; true } else { refetchInFlight = false; false }
            }
            if (!again) break
        }
    }

    // Backoff for re-reading an invalidated observation. The designed recovery
    // is the next coherent push and the first retry is deliberately not
    // immediate — an unavailable projection re-read at once would hammer a
    // mirror that just said it cannot answer. But a push is not guaranteed:
    // `exchange.updated` has no durable log, and the server drops an
    // enrichment that exceeds its budget without a deliverySeq, so nothing
    // client-side can see that loss. Doubles from 1s to a 30s ceiling (±20%
    // jitter) and stops the moment any structural state is applied.
    scheduleRecovery = {
        if (!stopped.get()) {
            recoveryJob?.cancel()
            val base = minOf(30_000.0, 1_000.0 * Math.pow(2.0, recoveryAttempt.toDouble()))
            val delayMs = (base + base * 0.2 * (Math.random() * 2 - 1)).toLong().coerceAtLeast(0)
            recoveryAttempt = minOf(recoveryAttempt + 1, 10)
            recoveryJob = scope.launch {
                delay(delayMs)
                if (stopped.get() || structural.value != null) return@launch
                refetch()
                if (stopped.get() || structural.value != null) return@launch
                synchronized(observationLock) { if (structural.value == null) scheduleRecovery() }
            }
        }
    }

    val initial = getExchangeState(objectId)
    if (armExpiry(initial, 0)) {
        structural.value = initial
        stream.exchangeStateMut.value = initial
        visible.observe(initial)
        stream.setState(WatchStreamState.CONNECTED)
    }

    jobs += scope.launch {
        ws.statusStream.collect { s ->
            if (s == ConnectionStatus.DISCONNECTED && stream.state.value != WatchStreamState.LOADING) {
                visible.invalidate()
                stream.setState(WatchStreamState.RECONNECTING)
            } else if (s == ConnectionStatus.CONNECTED && stream.state.value == WatchStreamState.RECONNECTING) {
                refetch()
            }
        }
    }
    // A hole in the server-assigned deliverySeq, or a server resync marker,
    // means at least one frame for this connection was lost, and there is no
    // durable log to replay `exchange.updated` from.
    val gapId = ws.onGap { visible.invalidate(); scope.launch { refetch() } }
    stream.refreshAction = { if (!stopped.get()) scope.launch { refetch() } }
    val refresherId = registerExchangeStateRefresher(objectId) { stream.refresh() }

    val fillObserverId = ws.observePositionFills { fill, event ->
        if (event.entityId == objectId || event.entityPath == objectPath) visible.observeFill(fill.market, fill.orderOperationId, fill.orderId, fill.createdAt)
    }

    ws.acquireMids(exchange)
    ws.watchPath(objectPath)

    jobs += scope.launch {
        ws.exchangeNotifications().collect { event ->
            if (event.entityId != objectId && event.entityPath != objectPath) return@collect
            val epoch = synchronized(observationLock) { ++observationEpoch }
            if (event.exchangeStateUnavailable == true) {
                synchronized(observationLock) { invalidate() }
                return@collect
            }
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
            applyObservation(structuralState, epoch)
        }
    }

    jobs += scope.launch {
        ws.midsEvents().collect { m ->
            mids.update { it + m }
            synchronized(observationLock) {
                val base = structural.value
                if (base != null) {
                    val value = base.revalued(mids.value)
                    stream.push(value); visible.observe(value)
                }
            }
        }
    }

    // Capability changes have no fill event on a quiet account.
    jobs += scope.launch {
        while (true) {
            delay((structural.value?.stateRefreshIntervalMs ?: 30000L).coerceIn(5000L, 60000L))
            if ((structural.value?.stateRefreshIntervalMs ?: 0L) <= 0L) continue
            refetch()
        }
    }

    stream.stopAction = {
        stopped.set(true)
        synchronized(observationLock) { expiryJob?.cancel(); recoveryJob?.cancel(); observationEpoch++ }
        jobs.forEach { it.cancel() }
        ws.removePositionFillObserver(fillObserverId)
        ws.removeGapHandler(gapId)
        unregisterExchangeStateRefresher(objectId, refresherId)
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
 * `fill.previewed` (instant preview, matched by stable `fillId`) is replaced by
 * the authoritative `fill.recorded`. A convergence timeout fires if a preview
 * does not receive its authoritative update within the window. On reconnect or
 * gap, performs bounded fresh-watch and paginated REST recovery (limit is page
 * size, at most 1,000 pages). Healthy watches do not poll. Call [FillWatchStream.stop] when done.
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
    val detail = getObjectDetail(objectId)
    val path = detail.`object`.path
    val stream = FillWatchStream()
    val lock = ReentrantLock()
    val stopped = AtomicBoolean(false)
    val timers = HashMap<String, Job>()
    val revision = java.util.concurrent.atomic.AtomicLong(0)
    val requests = kotlinx.coroutines.channels.Channel<Long>(kotlinx.coroutines.channels.Channel.CONFLATED)
    val jobs = mutableListOf<Job>()
    fun requestRecovery() { if (!stopped.get()) requests.trySend(revision.incrementAndGet()) }
    // UNDISPATCHED registers the shared-flow collector before watch/REST can emit.
    jobs += scope.launch(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) {
        ws.events.collect { event ->
            if (stopped.get() || !(event.entityPath == path || (event.entityPath == null && event.entityId == objectId))) return@collect
            val fill = if (event.type == "fill.recorded") event.recordedFill
                else if (event.type == "fill.previewed") event.fill?.let { preview ->
                    Fill(id = preview.id.value, fillId = preview.id.value, orderId = preview.orderId.value,
                        market = preview.market, side = preview.side, size = preview.size, price = preview.price,
                        fee = preview.fee, builderFee = preview.builderFee, realizedPnl = preview.realizedPnl,
                        isLiquidation = preview.isLiquidation, createdAt = preview.createdAt)
                } else null
            if (fill == null || (market != null && fill.market != market)) return@collect
            val key = fill.fillId ?: fill.id
            lock.withLock {
                stream.fillsMut.value = mergeWatchedFills(stream.fills.value, listOf(fill))
                if (!fill.operationId.isNullOrEmpty()) timers.remove(key)?.cancel()
                else if (stream.fills.value.none { (it.fillId ?: it.id) == key && !it.operationId.isNullOrEmpty() } && key !in timers) {
                    val correlation = event.correlationId ?: fill.orderId ?: key
                    timers[key] = scope.launch {
                        delay(FillWatchStream.CONVERGENCE_TIMEOUT_MS)
                        if (!stopped.get()) stream.fireConvergenceTimeout(correlation)
                    }
                }
            }
            stream.push(fill, event)
        }
    }
    jobs += scope.launch(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) {
        ws.statusStream.collect { status ->
            if (status == ConnectionStatus.DISCONNECTED && !stopped.get() && stream.state.value != WatchStreamState.LOADING) {
                stream.setState(WatchStreamState.RECONNECTING)
            }
        }
    }
    val gap = ws.onGap { requestRecovery() }
    val auth = ws.onAuthenticated { requestRecovery() }
    ws.watchPath(path)
    jobs += scope.launch {
        var completed = 0L
        for (requested in requests) {
            if (stopped.get()) break
            if (requested <= completed) continue
            var covered = requested
            for (attempt in 0 until 3) {
                var acknowledged = false
                try { kotlinx.coroutines.withTimeout(1000) { ws.recoverPathReady(path) }; acknowledged = true }
                catch (e: kotlinx.coroutines.TimeoutCancellationException) { /* bounded readiness failure */ }
                catch (e: kotlinx.coroutines.CancellationException) { throw e }
                catch (_: Exception) { /* the read may still recover recorded evidence */ }
                covered = revision.get()
                try {
                    val snapshot = fillWatchSnapshot(objectId, market, limit)
                    coroutineContext.ensureActive()
                    if (stopped.get()) return@launch
                    lock.withLock {
                        stream.fillsMut.value = mergeWatchedFills(stream.fills.value, snapshot)
                        stream.fills.value.filter { !it.operationId.isNullOrEmpty() }.forEach { timers.remove(it.fillId ?: it.id)?.cancel() }
                    }
                    if (acknowledged) { stream.setState(WatchStreamState.CONNECTED); break }
                } catch (e: kotlinx.coroutines.CancellationException) { throw e }
                catch (e: Exception) {
                    log.warning("watch", e, mapOf("objectId" to objectId)) { "fills recovery snapshot failed" }
                }
                if (attempt < 2) delay((attempt + 1) * 100L)
                else stream.setState(WatchStreamState.RECONNECTING)
            }
            completed = covered
        }
    }
    stream.stopAction = {
        stopped.set(true)
        requests.close()
        jobs.forEach { it.cancel() }
        lock.withLock { timers.values.forEach { it.cancel() }; timers.clear() }
        ws.removeGapHandler(gap)
        ws.removeAuthenticatedHandler(auth)
        ws.unwatchPath(path)
    }
    requestRecovery()
    try { stream.ready() } catch (e: kotlinx.coroutines.CancellationException) {
        kotlinx.coroutines.withContext(kotlinx.coroutines.NonCancellable) { stream.stop() }
        throw e
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

// MARK: - watchOI

/**
 * Subscribe to real-time open-interest + 24h-notional bar updates for one or
 * more markets. OI is a Tier-3 ambient stream (slow-moving, self-correcting,
 * no gap recovery). Each [OIEvent] carries a single bar; `isClosed` is true on
 * a finalized (rolled-over) bucket. Call [OIWatchStream.stop] when done.
 *
 * @param coins Canonical coin IDs to watch (e.g. `["hl:0:BTC", "hl:0:ETH"]`).
 * @param intervals OI intervals (defaults to 1m + 5m).
 */
public suspend fun Arca.watchOI(
    coins: List<String>,
    intervals: List<CandleInterval> = listOf(CandleInterval.ONE_MINUTE, CandleInterval.FIVE_MINUTES),
): OIWatchStream {
    ws.ensureConnected()

    val stream = OIWatchStream()
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

    ws.acquireOI(coins, intervals)

    jobs += scope.launch {
        ws.oiEvents().collect { event: OIEvent ->
            if (coinSet.isEmpty() || coinSet.contains(event.market)) {
                stream.push(event)
            }
        }
    }

    stream.stopAction = {
        jobs.forEach { it.cancel() }
        ws.releaseOI(coins, intervals)
    }
    return stream
}

// MARK: - watchPrices

/**
 * Watch real-time mid prices. Each update is a full snapshot of all known prices.
 *
 * Returns once prices are available, so [MarketPriceStream.prices] is
 * populated on return — for the first subscriber that is the server's
 * snapshot, and for every later one it is the map the manager retained from it
 * (the subscription is ref-counted, so the server only sends a snapshot per
 * subscribe). Call [MarketPriceStream.stop] when done.
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
 *
 * [MaxOrderSizeWatchStream.activeAssetData] can still be `null` when this
 * returns — it needs both an exchange state and a mark for the selected
 * market, and a market the venue prices nowhere has neither. `state` stays
 * `LOADING` and [MaxOrderSizeWatchStream.pendingReason] says which input is
 * missing. Hold sizing controls in a loading state while it is set; an empty
 * rail reads to the user as an empty account.
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
    // Fallback mark for the selected market, used only when the live mids map
    // has no entry for it. The same response already supplies the spread
    // ratios below; reading its `markPx` costs nothing and is the difference
    // between a ticket that can size the order and one that waits for a quiet
    // market to print its next mid.
    var seedMark: String? = null
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
            seedMark = data.markPx
            data.bidPx?.toDoubleOrNull()?.let { if (it > 0) bidRatio = it / mid }
            data.askPx?.toDoubleOrNull()?.let { if (it > 0) askRatio = it / mid }
        }
    }

    val exchangeStateBox = MutableStateFlow<ExchangeState?>(initialExchangeState)
    val observationLock = Any()
    var observationEpoch = 0L
    val jobs = mutableListOf<Job>()

    fun recompute(): ActiveAssetData? {
        val exState = exchangeStateBox.value ?: run {
            stream.pendingReasonMut.value = MaxOrderSizePendingReason.AWAITING_EXCHANGE_STATE
            return null
        }
        val mids = priceStream.prices.value
        // Live mid wins; the snapshot mark only covers a market the mids map
        // has not carried yet.
        val markPx = (mids[options.market] ?: seedMark)?.toDoubleOrNull() ?: 0.0
        if (markPx <= 0) {
            stream.pendingReasonMut.value = MaxOrderSizePendingReason.AWAITING_MARK_PRICE
            return null
        }
        // Re-mark the book against current mids before deriving.
        //
        // Load-bearing for HIP-3 markets, not merely a freshness nicety: the
        // cross-dex reservation is `max(margin, rate * NOTIONAL)` of the
        // venue-native position, and notional is read off `positionValue`. A
        // book left at the marks of the last structural push holds the shared
        // pool still while the price it depends on moves, so a HIP-3 market's
        // buying power would sit at a stale number until the next fill or
        // funding event — hours, on a quiet account.
        //
        // `totalCollateralUsd` is spot cash and price-invariant by
        // construction, so re-marking positions is the whole of what moves.
        val marked = if (mids.isEmpty()) exState else exState.revalued(mids)
        val derived = deriveActiveAssetData(
            exchangeState = marked,
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
        stream.pendingReasonMut.value =
            if (derived == null) MaxOrderSizePendingReason.AWAITING_EXCHANGE_STATE else null
        return derived
    }

    suspend fun fetchServerActiveAssetData(): ActiveAssetData? {
        val data = runCatching {
            getActiveAssetData(
                objectId = options.objectId,
                market = options.market,
                applicationFeeTenthsBps = options.builderFeeBps,
                leverage = options.leverage,
            )
        }.getOrNull()
        stream.pendingReasonMut.value =
            if (data == null) MaxOrderSizePendingReason.AWAITING_EXCHANGE_STATE else null
        return data
    }

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
            val epoch = synchronized(observationLock) { ++observationEpoch }
            val nextState = event.exchangeState ?: run {
                runCatching { getExchangeState(options.objectId) }.getOrNull() ?: return@collect
            }
            coroutineContext.ensureActive()
            val accepted = synchronized(observationLock) {
                if (epoch != observationEpoch) false else {
                    exchangeStateBox.value = nextState
                    true
                }
            }
            if (!accepted) return@collect
            val data = if (nextState.pricingMode == network.arca.sdk.models.PricingMode.SERVER) {
                fetchServerActiveAssetData()
            } else {
                recompute()
            }
            coroutineContext.ensureActive()
            synchronized(observationLock) {
                if (epoch == observationEpoch && data != null) {
                    stream.push(data)
                    stream.setState(WatchStreamState.CONNECTED)
                }
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

    // Capability changes have no fill event on a quiet account.
    jobs += scope.launch {
        while (true) {
            delay((exchangeStateBox.value?.stateRefreshIntervalMs ?: 30000L).coerceIn(5000L, 60000L))
            if ((exchangeStateBox.value?.stateRefreshIntervalMs ?: 0L) <= 0L) continue
            // A delayed REST refresh must not replace a newer exchange event.
            val epoch = synchronized(observationLock) { observationEpoch }
            val fresh = runCatching { getExchangeState(options.objectId) }.getOrNull()
            coroutineContext.ensureActive()
            if (fresh == null) continue
            val accepted = synchronized(observationLock) {
                if (epoch != observationEpoch) false else {
                    exchangeStateBox.value = fresh
                    true
                }
            }
            if (!accepted) continue
            val data = if (fresh.pricingMode == network.arca.sdk.models.PricingMode.SERVER) fetchServerActiveAssetData() else recompute()
            coroutineContext.ensureActive()
            synchronized(observationLock) {
                if (epoch == observationEpoch && data != null) {
                    stream.push(data)
                    stream.setState(WatchStreamState.CONNECTED)
                }
            }
        }
    }

    stream.stopAction = {
        jobs.forEach { it.cancel() }
        priceStream.stop()
        ws.unwatchPath(objectPath)
    }
    return stream
}
