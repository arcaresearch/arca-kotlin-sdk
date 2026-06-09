package network.arca.sdk

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import network.arca.sdk.models.ActiveAssetData
import network.arca.sdk.models.ArcaBalance
import network.arca.sdk.models.Candle
import network.arca.sdk.models.CandleChartUpdate
import network.arca.sdk.models.CandleEvent
import network.arca.sdk.models.EquityChartUpdate
import network.arca.sdk.models.EventEnvelope
import network.arca.sdk.models.ExchangeState
import network.arca.sdk.models.Fill
import network.arca.sdk.models.FundingPayment
import network.arca.sdk.models.LoadRangeResult
import network.arca.sdk.models.ObjectValuation
import network.arca.sdk.models.Operation
import network.arca.sdk.models.OrderSide
import network.arca.sdk.models.PathAggregation
import network.arca.sdk.models.RealmEvent
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

// MARK: - Shared lifecycle types

/**
 * Lifecycle state for a watch stream. Streams follow
 * `LOADING → CONNECTED ⇄ RECONNECTING`; they never enter a terminal error state.
 */
public enum class WatchStreamState { LOADING, CONNECTED, RECONNECTING }

/** State of the initial REST history load for a candle-chart stream. */
public sealed class InitialHistoryState {
    public object Loading : InitialHistoryState()
    public data class Loaded(public val count: Int) : InitialHistoryState()
    public data class Failed(public val error: String) : InitialHistoryState()
}

/** Snapshot of balances for a single object. */
public data class BalanceSnapshot(
    public val entityId: String,
    public val entityPath: String? = null,
    public val balances: List<ArcaBalance>,
)

/** Thread-safe registry of update callbacks keyed by an opaque token. */
internal class CallbackRegistry<T> {
    private val lock = ReentrantLock()
    private val handlers = LinkedHashMap<UUID, (T) -> Unit>()

    fun add(handler: (T) -> Unit): () -> Unit {
        val id = UUID.randomUUID()
        lock.withLock { handlers[id] = handler }
        return { lock.withLock { handlers.remove(id) } }
    }

    fun fire(value: T) {
        val snapshot = lock.withLock { handlers.values.toList() }
        snapshot.forEach { it(value) }
    }

    fun clear() = lock.withLock { handlers.clear() }
}

/** Base class providing shared `state`, `stop`, and `ready` plumbing. */
public abstract class BaseWatchStream internal constructor() {
    internal val stateMut: MutableStateFlow<WatchStreamState> = MutableStateFlow(WatchStreamState.LOADING)

    /** Current lifecycle state of the stream. */
    public val state: StateFlow<WatchStreamState> get() = stateMut.asStateFlow()

    private val stopped = AtomicBoolean(false)
    internal var stopAction: (() -> Unit)? = null

    /** Stop listening and release subscriptions. Idempotent. */
    public fun stop() {
        if (stopped.compareAndSet(false, true)) {
            stopAction?.invoke()
        }
    }

    /** Suspends until the first snapshot has been received. Never throws. */
    public suspend fun ready() {
        state.first { it != WatchStreamState.LOADING }
    }

    internal fun setState(newState: WatchStreamState) {
        stateMut.value = newState
    }
}

private fun <T> updatesFlow(): MutableSharedFlow<T> = MutableSharedFlow(
    replay = 0,
    extraBufferCapacity = 256,
    onBufferOverflow = BufferOverflow.DROP_OLDEST,
)

// MARK: - OperationWatchStream

/** A stream of real-time operation events. */
public class OperationWatchStream internal constructor() : BaseWatchStream() {
    internal val operationsMut: MutableStateFlow<List<Operation>> = MutableStateFlow(emptyList())
    internal val updatesMut: MutableSharedFlow<Pair<Operation, RealmEvent>> = updatesFlow()
    private val callbacks = CallbackRegistry<Pair<Operation, RealmEvent>>()
    internal var trackingScope: CoroutineScope? = null

    /** Operations list, populated on first snapshot and refreshed on reconnect. */
    public val operations: StateFlow<List<Operation>> get() = operationsMut.asStateFlow()

    /** A stream of operation create/update events. */
    public val updates: Flow<Pair<Operation, RealmEvent>> get() = updatesMut.asSharedFlow()

    /** Register a callback invoked on each operation event. Returns an unsubscribe function. */
    public fun onUpdate(handler: (Operation, RealmEvent) -> Unit): () -> Unit =
        callbacks.add { (op, ev) -> handler(op, ev) }

    internal fun push(op: Operation, event: RealmEvent) {
        updatesMut.tryEmit(op to event)
        callbacks.fire(op to event)
    }

    /**
     * Track a mutation's operation: when the HTTP response arrives, the operation
     * is immediately injected into [operations], giving instant UI feedback
     * before the server-side WebSocket event.
     */
    internal fun <T : OperationResponse> trackSubmission(handle: OperationHandle<T>) {
        val scope = trackingScope ?: return
        scope.launch {
            val response = runCatching { handle.submitted() }.getOrNull() ?: return@launch
            val op = response.operation
            operationsMut.value = buildList {
                if (operationsMut.value.none { it.id == op.id }) add(op)
                addAll(operationsMut.value)
            }
        }
    }
}

// MARK: - BalanceWatchStream

/** A stream of real-time balance updates. */
public class BalanceWatchStream internal constructor() : BaseWatchStream() {
    internal val balancesMut: MutableStateFlow<Map<String, BalanceSnapshot>> = MutableStateFlow(emptyMap())
    internal val updatesMut: MutableSharedFlow<Pair<String, RealmEvent>> = updatesFlow()
    private val callbacks = CallbackRegistry<Pair<String, RealmEvent>>()

    /** Current balances by object ID, updated as events arrive. */
    public val balances: StateFlow<Map<String, BalanceSnapshot>> get() = balancesMut.asStateFlow()

    /** A stream of balance update events. */
    public val updates: Flow<Pair<String, RealmEvent>> get() = updatesMut.asSharedFlow()

    /** Register a callback invoked on each balance update. Returns an unsubscribe function. */
    public fun onUpdate(handler: (String, RealmEvent) -> Unit): () -> Unit =
        callbacks.add { (id, ev) -> handler(id, ev) }

    internal fun push(entityId: String, event: RealmEvent) {
        updatesMut.tryEmit(entityId to event)
        callbacks.fire(entityId to event)
    }
}

// MARK: - ObjectWatchStream

/** A stream of real-time valuation updates for a single Arca object. */
public class ObjectWatchStream internal constructor(public val path: String) : BaseWatchStream() {
    internal val watchIdMut: MutableStateFlow<String?> = MutableStateFlow(null)
    internal val valuationMut: MutableStateFlow<ObjectValuation?> = MutableStateFlow(null)
    internal val updatesMut: MutableSharedFlow<ObjectValuation> = updatesFlow()
    private val callbacks = CallbackRegistry<ObjectValuation>()

    /** Watch ID assigned by the server (used for unsubscribe). */
    public val watchId: StateFlow<String?> get() = watchIdMut.asStateFlow()

    /** Current valuation (updated on each server push). */
    public val valuation: StateFlow<ObjectValuation?> get() = valuationMut.asStateFlow()

    /** A stream of valuation updates. */
    public val updates: Flow<ObjectValuation> get() = updatesMut.asSharedFlow()

    /** Register a callback invoked on each valuation update. Returns an unsubscribe function. */
    public fun onUpdate(handler: (ObjectValuation) -> Unit): () -> Unit = callbacks.add(handler)

    internal fun push(valuation: ObjectValuation) {
        valuationMut.value = valuation
        updatesMut.tryEmit(valuation)
        callbacks.fire(valuation)
    }
}

// MARK: - ObjectsWatchStream

/** Merges multiple [ObjectWatchStream] instances into one map keyed by object path. */
public class ObjectsWatchStream internal constructor(
    public val childStreams: List<ObjectWatchStream>,
) : BaseWatchStream() {
    internal val valuationsMut: MutableStateFlow<Map<String, ObjectValuation>> = MutableStateFlow(emptyMap())
    internal val updatesMut: MutableSharedFlow<Map<String, ObjectValuation>> = updatesFlow()
    private val callbacks = CallbackRegistry<Map<String, ObjectValuation>>()

    /** Latest valuations keyed by Arca object path. */
    public val valuations: StateFlow<Map<String, ObjectValuation>> get() = valuationsMut.asStateFlow()

    /** A stream of merged valuation maps. */
    public val updates: Flow<Map<String, ObjectValuation>> get() = updatesMut.asSharedFlow()

    /** Register a callback invoked on each merged snapshot. Returns an unsubscribe function. */
    public fun onUpdate(handler: (Map<String, ObjectValuation>) -> Unit): () -> Unit = callbacks.add(handler)

    internal fun push(snapshot: Map<String, ObjectValuation>) {
        valuationsMut.value = snapshot
        updatesMut.tryEmit(snapshot)
        callbacks.fire(snapshot)
    }
}

// MARK: - AggregationWatchStream

/** A stream of real-time aggregation updates with client-side revaluation. */
public class AggregationWatchStream internal constructor(public val watchId: String) : BaseWatchStream() {
    internal val aggregationMut: MutableStateFlow<PathAggregation?> = MutableStateFlow(null)
    internal val updatesMut: MutableSharedFlow<PathAggregation> = updatesFlow()
    private val callbacks = CallbackRegistry<PathAggregation>()

    /** Current aggregation (updated on structural changes and price ticks). */
    public val aggregation: StateFlow<PathAggregation?> get() = aggregationMut.asStateFlow()

    /** A stream of revalued aggregation updates. */
    public val updates: Flow<PathAggregation> get() = updatesMut.asSharedFlow()

    /** Register a callback invoked on each aggregation update. Returns an unsubscribe function. */
    public fun onUpdate(handler: (PathAggregation) -> Unit): () -> Unit = callbacks.add(handler)

    internal fun push(aggregation: PathAggregation) {
        aggregationMut.value = aggregation
        updatesMut.tryEmit(aggregation)
        callbacks.fire(aggregation)
    }
}

// MARK: - MarketPriceStream

/** A stream of real-time mid prices. */
public class MarketPriceStream internal constructor() : BaseWatchStream() {
    internal val pricesMut: MutableStateFlow<Map<String, String>> = MutableStateFlow(emptyMap())
    internal val updatesMut: MutableSharedFlow<Map<String, String>> = updatesFlow()

    /** Current mid prices, populated on first snapshot and refreshed on reconnect. */
    public val prices: StateFlow<Map<String, String>> get() = pricesMut.asStateFlow()

    /** A stream of mid price updates (each update is a full snapshot of all prices). */
    public val updates: Flow<Map<String, String>> get() = updatesMut.asSharedFlow()

    internal fun push(prices: Map<String, String>) {
        pricesMut.value = prices
        updatesMut.tryEmit(prices)
    }
}

// MARK: - EquityChartStream

/** Merges historical equity data with a live aggregation stream. */
public class EquityChartStream internal constructor() : BaseWatchStream() {
    internal val chartMut: MutableStateFlow<List<network.arca.sdk.models.EquityPoint>> = MutableStateFlow(emptyList())
    internal val updatesMut: MutableSharedFlow<EquityChartUpdate> = updatesFlow()

    /** Current chart points (historical + live tail). */
    public val chart: StateFlow<List<network.arca.sdk.models.EquityPoint>> get() = chartMut.asStateFlow()

    /** A stream of chart updates. */
    public val updates: Flow<EquityChartUpdate> get() = updatesMut.asSharedFlow()

    internal fun push(points: List<network.arca.sdk.models.EquityPoint>) {
        chartMut.value = points
        updatesMut.tryEmit(EquityChartUpdate(points))
    }
}

// MARK: - PnlChartStream

/** Merges historical P&L data with a live aggregation stream and operation events. */
public class PnlChartStream internal constructor() : BaseWatchStream() {
    internal val chartMut: MutableStateFlow<List<network.arca.sdk.models.PnlPoint>> = MutableStateFlow(emptyList())
    internal val updatesMut: MutableSharedFlow<network.arca.sdk.models.PnlChartUpdate> = updatesFlow()

    /** Current P&L chart points (historical + live tail). */
    public val chart: StateFlow<List<network.arca.sdk.models.PnlPoint>> get() = chartMut.asStateFlow()

    /** A stream of P&L chart updates. */
    public val updates: Flow<network.arca.sdk.models.PnlChartUpdate> get() = updatesMut.asSharedFlow()

    internal fun push(update: network.arca.sdk.models.PnlChartUpdate) {
        chartMut.value = update.points
        updatesMut.tryEmit(update)
    }
}

// MARK: - CandleWatchStream

/** A stream of real-time candle updates. */
public class CandleWatchStream internal constructor() : BaseWatchStream() {
    internal val updatesMut: MutableSharedFlow<CandleEvent> = updatesFlow()

    /** A stream of candle events (both closed and in-progress). */
    public val updates: Flow<CandleEvent> get() = updatesMut.asSharedFlow()

    internal fun push(event: CandleEvent) {
        updatesMut.tryEmit(event)
    }
}

// MARK: - CandleChartStream

/** Merges historical candle data with real-time WebSocket candle events. */
public class CandleChartStream internal constructor() : BaseWatchStream() {
    internal val historySnapshotMut: MutableStateFlow<InitialHistoryState> = MutableStateFlow(InitialHistoryState.Loading)
    internal val candlesMut: MutableStateFlow<List<Candle>> = MutableStateFlow(emptyList())
    internal val updatesMut: MutableSharedFlow<CandleChartUpdate> = updatesFlow()
    private val callbacks = CallbackRegistry<CandleChartUpdate>()

    /** State of the initial REST history load. */
    public val historySnapshot: StateFlow<InitialHistoryState> get() = historySnapshotMut.asStateFlow()

    /** Current candle array (historical + live), sorted by `t`, deduped. */
    public val candles: StateFlow<List<Candle>> get() = candlesMut.asStateFlow()

    /** A stream of chart updates. */
    public val updates: Flow<CandleChartUpdate> get() = updatesMut.asSharedFlow()

    /** Ensure candles are loaded for the given time range (only requests the gaps). */
    internal var ensureRangeAction: (suspend (Long, Long) -> LoadRangeResult)? = null

    /** Load older candles backwards from the current earliest candle. */
    internal var loadMoreAction: (suspend (Int) -> LoadRangeResult)? = null

    /** Register a callback invoked on each chart update. Returns an unsubscribe function. */
    public fun onUpdate(handler: (CandleChartUpdate) -> Unit): () -> Unit = callbacks.add(handler)

    /** Ensure candles for `[start, end]` (epoch ms) are loaded. Idempotent and coalesced. */
    public suspend fun ensureRange(start: Long, end: Long): LoadRangeResult =
        ensureRangeAction?.invoke(start, end) ?: LoadRangeResult(0, candlesMut.value.size, start, end, false)

    /** Load `count` older candle periods backwards from the current earliest candle. */
    public suspend fun loadMore(count: Int = 300): LoadRangeResult =
        loadMoreAction?.invoke(count) ?: LoadRangeResult(0, candlesMut.value.size, 0, 0, false)

    internal fun push(update: CandleChartUpdate) {
        candlesMut.value = update.candles
        updatesMut.tryEmit(update)
        callbacks.fire(update)
    }
}

// MARK: - MaxOrderSizeWatchStream

/** Options for `Arca.watchMaxOrderSize`. */
public data class MaxOrderSizeWatchOptions(
    public val objectId: String,
    public val market: String,
    public val side: OrderSide,
    public val leverage: Int,
    public val applicationFeeTenthsBps: Int? = null,
    public val szDecimals: Int = 5,
    /** HIP-3 fee multiplier; when null, auto-fetched from tickers. */
    public val feeScale: Double? = null,
    /** Asset's base maintenance margin rate; when null, auto-fetched. */
    public val maintenanceMarginRate: String? = null,
) {
    public val builderFeeBps: Int get() = applicationFeeTenthsBps ?: 0
}

/** A stream that recomputes [ActiveAssetData] whenever exchange state or mid prices change. */
public class MaxOrderSizeWatchStream internal constructor() : BaseWatchStream() {
    internal val activeAssetDataMut: MutableStateFlow<ActiveAssetData?> = MutableStateFlow(null)
    internal val updatesMut: MutableSharedFlow<ActiveAssetData> = updatesFlow()

    /** Latest derived active asset data (null until first computation). */
    public val activeAssetData: StateFlow<ActiveAssetData?> get() = activeAssetDataMut.asStateFlow()

    /** A stream of recomputed active asset data. */
    public val updates: Flow<ActiveAssetData> get() = updatesMut.asSharedFlow()

    internal fun push(data: ActiveAssetData) {
        activeAssetDataMut.value = data
        updatesMut.tryEmit(data)
    }
}

// MARK: - ExchangeStateWatchStream

/** A stream of real-time exchange state updates for an Arca exchange object. */
public class ExchangeStateWatchStream internal constructor() : BaseWatchStream() {
    internal val exchangeStateMut: MutableStateFlow<ExchangeState?> = MutableStateFlow(null)
    internal val updatesMut: MutableSharedFlow<ExchangeState> = updatesFlow()

    /** Current exchange state (positions, orders, margin). */
    public val exchangeState: StateFlow<ExchangeState?> get() = exchangeStateMut.asStateFlow()

    /** A stream of exchange state updates. */
    public val updates: Flow<ExchangeState> get() = updatesMut.asSharedFlow()

    internal fun push(state: ExchangeState) {
        exchangeStateMut.value = state
        updatesMut.tryEmit(state)
    }
}

// MARK: - FundingWatchStream

/** A stream of real-time funding payment events for an exchange Arca object. */
public class FundingWatchStream internal constructor() : BaseWatchStream() {
    internal val updatesMut: MutableSharedFlow<Pair<FundingPayment, EventEnvelope>> = updatesFlow()

    /** A stream of funding payment events. */
    public val updates: Flow<Pair<FundingPayment, EventEnvelope>> get() = updatesMut.asSharedFlow()

    internal fun push(payment: FundingPayment, envelope: EventEnvelope) {
        updatesMut.tryEmit(payment to envelope)
    }
}

// MARK: - FillWatchStream

/** A stream of platform-level trade history for an exchange Arca object. */
public class FillWatchStream internal constructor() : BaseWatchStream() {
    internal val fillsMut: MutableStateFlow<List<Fill>> = MutableStateFlow(emptyList())
    internal val updatesMut: MutableSharedFlow<Pair<Fill, RealmEvent>> = updatesFlow()
    private val convergenceCallbacks = CallbackRegistry<String>()

    /** Running list of fills, populated on initial fetch and updated live (merged view). */
    public val fills: StateFlow<List<Fill>> get() = fillsMut.asStateFlow()

    /** A stream of every fill transition (yields both preview and recorded phases). */
    public val updates: Flow<Pair<Fill, RealmEvent>> get() = updatesMut.asSharedFlow()

    /** Register a callback for convergence timeouts. Returns an unsubscribe function. */
    public fun onConvergenceTimeout(handler: (String) -> Unit): () -> Unit = convergenceCallbacks.add(handler)

    internal fun push(fill: Fill, event: RealmEvent) {
        updatesMut.tryEmit(fill to event)
    }

    internal fun fireConvergenceTimeout(correlationId: String) {
        convergenceCallbacks.fire(correlationId)
    }

    public companion object {
        /** Convergence timeout for preview fills awaiting authoritative updates (ms). */
        public const val CONVERGENCE_TIMEOUT_MS: Long = 10_000
    }
}
