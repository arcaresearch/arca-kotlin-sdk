package network.arca.sdk

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.Candle
import network.arca.sdk.models.CandleEvent
import network.arca.sdk.models.CandleInterval
import network.arca.sdk.models.OIEvent
import network.arca.sdk.models.ConnectionStatus
import network.arca.sdk.models.EventType
import network.arca.sdk.models.ExchangeState
import network.arca.sdk.models.Fill
import network.arca.sdk.models.FundingPayment
import network.arca.sdk.models.ObjectValuation
import network.arca.sdk.models.Operation
import network.arca.sdk.models.PathAggregation
import network.arca.sdk.models.ProjectedValuation
import network.arca.sdk.models.RealmEvent
import network.arca.sdk.models.SimFill
import network.arca.sdk.models.TypedEvent
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.min
import kotlin.math.pow
import kotlin.random.Random

/**
 * Host-app lifecycle bridge. Android consumers supply an implementation backed
 * by `ProcessLifecycleOwner`; pure-JVM usage can leave it null. [install] wires
 * foreground/background callbacks; [uninstall] tears them down.
 */
public interface AppLifecycleBridge {
    public fun install(onForeground: () -> Unit, onBackground: () -> Unit)
    public fun uninstall()
}

/**
 * WebSocket manager for real-time Arca events, built on OkHttp's WebSocket.
 *
 * Handles authentication, ref-counted channel subscriptions, automatic
 * reconnection with exponential backoff, an application-level heartbeat for
 * half-open detection, delivery-gap detection, and host lifecycle resume
 * probing. Events are delivered via cold [Flow]s derived from an internal
 * [SharedFlow] bus; multiple consumers can collect concurrently.
 *
 * All mutable state is guarded by a single [ReentrantLock], mirroring the Swift
 * actor's serialized access.
 */
public class WebSocketManager internal constructor(
    baseUrl: String,
    token: String,
    private val realmId: String,
    httpClient: OkHttpClient,
    private val getToken: (suspend () -> String)? = null,
    private val maxReconnectDelaySeconds: Double = 30.0,
    private val log: ArcaLogger = ArcaLogger.disabled,
    private val lifecycleBridge: AppLifecycleBridge? = null,
    // Null means "unset", which takes the default. 0 means "never rotate" and is
    // a deliberate opt-out, so the two cannot share a representation.
    private val connectionLifetimeMs: Long? = null,
    // Seams for tests, which need to drive two sockets at once, control the
    // order frames arrive in across them, and reach the abandon path without
    // waiting out the real budget.
    private val handoffTimeoutMs: Long = HANDOFF_TIMEOUT_MS,
    private val socketFactory: WebSocket.Factory = httpClient,
) {
    private val wsUrl = baseUrl.trimEnd('/').toHttpUrl().newBuilder()
        .addPathSegments("api/v1/ws").build()

    private val lock = ReentrantLock()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    @Volatile private var token: String = token
    @Volatile private var webSocket: WebSocket? = null

    /** The replacement being warmed up alongside [webSocket] during a rotation. */
    @Volatile private var handoffWebSocket: WebSocket? = null

    private var subscribedMids: Pair<String, List<String>>? = null
    private var subscribedCandles: Pair<List<String>, List<CandleInterval>>? = null
    private var subscribedOI: Pair<List<String>, List<CandleInterval>>? = null
    private var shouldReconnect = false
    private var reconnectAttempt = 0
    private var reconnectJob: Job? = null
    private var rotationJob: Job? = null
    private var handoffTimeoutJob: Job? = null
    private var serverLifetimeMs: Long? = null

    private val pathRefs = HashMap<String, Int>()
    private var midsRefs = 0
    private var midsExchange = "sim"
    private val candleRefCoins = HashMap<String, MutableSet<String>>()
    private val oiRefCoins = HashMap<String, MutableSet<String>>()
    private val chartHistoryWatches = HashMap<String, ChartWatch>()

    // Watches created out-of-band (REST POST /aggregations/watch) that this
    // socket registered for delivery. Delivery is ownership-gated
    // server-side, so these must be re-attached on every reconnect or the
    // watch goes silent.
    private val attachedWatches = LinkedHashSet<String>()

    // Projection watch request/reply state. Replies are matched to callers by
    // requestId; a request issued while disconnected parks in
    // [pendingProjectionSends] until the next successful auth flushes it.
    private val pendingProjectionRequests = HashMap<String, CompletableDeferred<ProjectionWatchCreated>>()
    private val pendingProjectionSends = HashMap<String, String>()
    private var nextProjectionRequestId = 0

    private val unsubJobs = HashMap<String, Job>()
    private var idleDisconnectJob: Job? = null

    private var pingJob: Job? = null
    @Volatile private var lastMessageAtMs: Long = System.currentTimeMillis()
    private var resumeProbeJob: Job? = null
    private var hiddenAtMs: Long? = null
    private var lifecycleInstalled = false

    private var lastDeliverySeq = 0

    private val gapHandlers = ConcurrentHashMap<UUID, (Int) -> Unit>()
    private val resumeHandlers = ConcurrentHashMap<UUID, (Double) -> Unit>()
    private val authenticatedHandlers = ConcurrentHashMap<UUID, () -> Unit>()
    private val rotatedHandlers = ConcurrentHashMap<UUID, () -> Unit>()

    private val bus = MutableSharedFlow<RealmEvent>(
        replay = 0,
        extraBufferCapacity = 1024,
        onBufferOverflow = kotlinx.coroutines.channels.BufferOverflow.DROP_OLDEST,
    )
    private val statusFlow = MutableStateFlow(ConnectionStatus.DISCONNECTED)
    private val resumeFlow = MutableSharedFlow<Double>(extraBufferCapacity = 16)
    private val authenticatedFlow = MutableSharedFlow<Unit>(extraBufferCapacity = 16)
    private val rotatedFlow = MutableSharedFlow<Unit>(extraBufferCapacity = 16)

    private data class ChartWatch(val target: String, val kind: String, val objectId: String?)

    // MARK: - Token

    /**
     * Update the bearer token. If disconnected and reconnect is desired,
     * triggers an immediate reconnect with the new token.
     */
    public fun updateToken(newToken: String) {
        lock.withLock {
            token = newToken
            if (shouldReconnect && webSocket == null) {
                reconnectJob?.cancel()
                reconnectJob = null
                reconnectAttempt = 0
                doConnectLocked()
            }
        }
    }

    /** Current connection status. */
    public val status: ConnectionStatus
        get() = statusFlow.value

    // MARK: - Connection lifecycle

    /** Connect to the WebSocket. */
    public fun connect() {
        lock.withLock {
            shouldReconnect = true
            installLifecycleLocked()
            doConnectLocked()
        }
    }

    /** Connect only if not already connected or connecting. */
    public fun ensureConnected() {
        lock.withLock {
            if (webSocket != null) return
            shouldReconnect = true
            installLifecycleLocked()
            doConnectLocked()
        }
    }

    /** Force the WebSocket to disconnect and immediately reconnect. */
    public fun reconnect() {
        log.info("websocket") { "manual reconnect requested" }
        lock.withLock {
            reconnectJob?.cancel()
            reconnectJob = null
            reconnectAttempt = 0
            doConnectLocked()
        }
    }

    /** Disconnect and stop reconnecting. */
    public fun disconnect() {
        lock.withLock {
            shouldReconnect = false
            reconnectJob?.cancel(); reconnectJob = null
            resumeProbeJob?.cancel(); resumeProbeJob = null
            cancelRotationLocked()
            abortHandoffLocked()
            stopHeartbeatLocked()
            cancelIdleTimerLocked()
            unsubJobs.values.forEach { it.cancel() }
            unsubJobs.clear()
            webSocket?.cancel()
            webSocket = null
            setStatusLocked(ConnectionStatus.DISCONNECTED)
            removeLifecycleLocked()
        }
    }

    // MARK: - Subscriptions

    public fun subscribeMids(exchange: String, coins: List<String> = emptyList()) {
        lock.withLock {
            subscribedMids = exchange to coins
            sendMessage(authlessSubscribeMids(exchange, coins))
        }
    }

    public fun unsubscribeMids() {
        lock.withLock {
            subscribedMids = null
            sendMessage(buildJsonObject { put("action", "unsubscribe_mids") })
        }
    }

    public fun subscribeCandles(coins: List<String>, intervals: List<CandleInterval>) {
        lock.withLock {
            subscribedCandles = coins to intervals
            sendMessage(subscribeCandlesMsg(coins, intervals.map { it.wire }))
        }
    }

    public fun unsubscribeCandles() {
        lock.withLock {
            subscribedCandles = null
            sendMessage(buildJsonObject { put("action", "unsubscribe_candles") })
        }
    }

    public fun subscribeOI(coins: List<String>, intervals: List<CandleInterval>) {
        lock.withLock {
            subscribedOI = coins to intervals
            sendMessage(subscribeOIMsg(coins, intervals.map { it.wire }))
        }
    }

    public fun unsubscribeOI() {
        lock.withLock {
            subscribedOI = null
            sendMessage(buildJsonObject { put("action", "unsubscribe_oi") })
        }
    }

    // MARK: - Path watch management (ref-counted)

    public fun watchPath(path: String) {
        lock.withLock {
            cancelIdleTimerLocked()
            val prev = pathRefs[path] ?: 0
            pathRefs[path] = prev + 1
            if (prev == 0) {
                val timerKey = "path:$path"
                val pending = unsubJobs.remove(timerKey)
                if (pending != null) {
                    pending.cancel()
                } else {
                    ensureConnectedLocked()
                    sendMessage(buildJsonObject { put("action", "watch"); put("path", path) })
                }
            }
        }
    }

    public fun unwatchPath(path: String) {
        lock.withLock {
            val current = pathRefs[path] ?: 0
            if (current <= 1) {
                pathRefs.remove(path)
                val timerKey = "path:$path"
                unsubJobs[timerKey] = scope.launch {
                    delay(UNSUB_DEBOUNCE_MS)
                    if (!isActive) return@launch
                    finishPathUnwatch(path, timerKey)
                }
            } else {
                pathRefs[path] = current - 1
            }
        }
    }

    private fun finishPathUnwatch(path: String, timerKey: String) {
        lock.withLock {
            unsubJobs.remove(timerKey)
            if (!pathRefs.containsKey(path)) {
                sendMessage(buildJsonObject { put("action", "unwatch"); put("path", path) })
            }
            maybeStartIdleTimerLocked()
        }
    }

    public fun acquireMids(exchange: String) {
        lock.withLock {
            cancelIdleTimerLocked()
            midsExchange = exchange
            midsRefs += 1
            if (midsRefs == 1) {
                val pending = unsubJobs.remove("mids")
                if (pending != null) {
                    pending.cancel()
                } else {
                    ensureConnectedLocked()
                    subscribedMids = exchange to emptyList()
                    sendMessage(authlessSubscribeMids(exchange, emptyList()))
                }
            }
        }
    }

    public fun releaseMids() {
        lock.withLock {
            midsRefs = maxOf(0, midsRefs - 1)
            if (midsRefs == 0) {
                unsubJobs["mids"] = scope.launch {
                    delay(UNSUB_DEBOUNCE_MS)
                    if (!isActive) return@launch
                    finishMidsRelease()
                }
            }
        }
    }

    private fun finishMidsRelease() {
        lock.withLock {
            unsubJobs.remove("mids")
            if (midsRefs == 0) {
                subscribedMids = null
                sendMessage(buildJsonObject { put("action", "unsubscribe_mids") })
            }
            maybeStartIdleTimerLocked()
        }
    }

    public fun acquireCandles(coins: List<String>, intervals: List<CandleInterval>) {
        lock.withLock {
            cancelIdleTimerLocked()
            for (coin in coins) {
                val set = candleRefCoins.getOrPut(coin) { mutableSetOf() }
                intervals.forEach { set.add(it.wire) }
            }
            ensureConnectedLocked()
            syncCandleSubscriptionLocked()
        }
    }

    public fun releaseCandles(coins: List<String>, intervals: List<CandleInterval>) {
        lock.withLock {
            for (coin in coins) {
                val ivs = candleRefCoins[coin] ?: continue
                intervals.forEach { ivs.remove(it.wire) }
                if (ivs.isEmpty()) candleRefCoins.remove(coin)
            }
            unsubJobs["candles"] = scope.launch {
                delay(UNSUB_DEBOUNCE_MS)
                if (!isActive) return@launch
                finishCandleRelease()
            }
        }
    }

    private fun finishCandleRelease() {
        lock.withLock {
            unsubJobs.remove("candles")
            syncCandleSubscriptionLocked()
            maybeStartIdleTimerLocked()
        }
    }

    private fun syncCandleSubscriptionLocked(target: WebSocket? = webSocket) {
        if (candleRefCoins.isEmpty()) {
            subscribedCandles = null
            sendMessage(target, buildJsonObject { put("action", "unsubscribe_candles") })
            return
        }
        val allCoins = candleRefCoins.keys.toList()
        val allIntervals = mutableSetOf<String>()
        candleRefCoins.values.forEach { allIntervals.addAll(it) }
        val intervals = allIntervals.mapNotNull { CandleInterval.fromWire(it) }
        subscribedCandles = allCoins to intervals
        sendMessage(target, subscribeCandlesMsg(allCoins, intervals.map { it.wire }))
    }

    public fun acquireOI(coins: List<String>, intervals: List<CandleInterval>) {
        lock.withLock {
            cancelIdleTimerLocked()
            for (coin in coins) {
                val set = oiRefCoins.getOrPut(coin) { mutableSetOf() }
                intervals.forEach { set.add(it.wire) }
            }
            ensureConnectedLocked()
            syncOISubscriptionLocked()
        }
    }

    public fun releaseOI(coins: List<String>, intervals: List<CandleInterval>) {
        lock.withLock {
            for (coin in coins) {
                val ivs = oiRefCoins[coin] ?: continue
                intervals.forEach { ivs.remove(it.wire) }
                if (ivs.isEmpty()) oiRefCoins.remove(coin)
            }
            unsubJobs["oi"] = scope.launch {
                delay(UNSUB_DEBOUNCE_MS)
                if (!isActive) return@launch
                finishOIRelease()
            }
        }
    }

    private fun finishOIRelease() {
        lock.withLock {
            unsubJobs.remove("oi")
            syncOISubscriptionLocked()
            maybeStartIdleTimerLocked()
        }
    }

    private fun syncOISubscriptionLocked(target: WebSocket? = webSocket) {
        if (oiRefCoins.isEmpty()) {
            subscribedOI = null
            sendMessage(target, buildJsonObject { put("action", "unsubscribe_oi") })
            return
        }
        val allCoins = oiRefCoins.keys.toList()
        val allIntervals = mutableSetOf<String>()
        oiRefCoins.values.forEach { allIntervals.addAll(it) }
        val intervals = allIntervals.mapNotNull { CandleInterval.fromWire(it) }
        subscribedOI = allCoins to intervals
        sendMessage(target, subscribeOIMsg(allCoins, intervals.map { it.wire }))
    }

    // MARK: - Projection watches

    /**
     * Create a server-side projection watch and suspend until the server
     * replies with the initial snapshot. The watch is connection-scoped: it
     * dies with the socket and callers must re-create it on reconnect or
     * rotation (see [rotatedStream]).
     */
    public suspend fun createProjectionWatch(projection: String): ProjectionWatchCreated {
        val deferred = CompletableDeferred<ProjectionWatchCreated>()
        val requestId = lock.withLock {
            cancelIdleTimerLocked()
            ensureConnectedLocked()
            nextProjectionRequestId += 1
            val id = "proj-req-$nextProjectionRequestId"
            pendingProjectionRequests[id] = deferred
            if (statusFlow.value == ConnectionStatus.CONNECTED) {
                sendMessage(watchProjectionMsg(projection, id))
            } else {
                pendingProjectionSends[id] = projection
            }
            id
        }
        try {
            return withTimeout(PROJECTION_REQUEST_TIMEOUT_MS) { deferred.await() }
        } catch (e: TimeoutCancellationException) {
            lock.withLock {
                pendingProjectionRequests.remove(requestId)
                pendingProjectionSends.remove(requestId)
            }
            throw ArcaException.Unknown(
                "WS_REQUEST_TIMEOUT",
                "watch_projection '$projection': timed out waiting for the server reply",
            )
        }
    }

    /** Tear down a server-side projection watch. Best-effort; safe when disconnected. */
    public fun destroyProjectionWatch(watchId: String) {
        lock.withLock {
            sendMessage(buildJsonObject { put("action", "unwatch_projection"); put("watchId", watchId) })
            maybeStartIdleTimerLocked()
        }
    }

    public fun watchChartHistory(target: String, kind: String = "path", objectId: String? = null): String {
        val watchId = UUID.randomUUID().toString()
        lock.withLock {
            cancelIdleTimerLocked()
            chartHistoryWatches[watchId] = ChartWatch(target, kind, objectId)
            ensureConnectedLocked()
            sendMessage(watchChartHistoryMsg(watchId, target, kind, objectId))
        }
        return watchId
    }

    public fun unwatchChartHistory(watchId: String) {
        lock.withLock {
            chartHistoryWatches.remove(watchId)
            sendMessage(buildJsonObject { put("action", "unwatch_chart_history"); put("watchId", watchId) })
            maybeStartIdleTimerLocked()
        }
    }

    /**
     * Register a watch created out-of-band (`POST /aggregations/watch`) for
     * delivery on this socket.
     *
     * Delivery is ownership-gated server-side: a socket receives
     * `aggregation.updated` only for watches it registered. Without this a
     * REST-created watch produces no events. The server re-authorizes the
     * watch's sources against this connection's credential, so attaching can
     * never widen access.
     */
    public fun attachAggregationWatch(watchId: String) {
        if (watchId.isEmpty()) return
        lock.withLock {
            cancelIdleTimerLocked()
            attachedWatches.add(watchId)
            ensureConnectedLocked()
            sendMessage(attachAggregationWatchMsg(watchId))
        }
    }

    /** Stop delivery of a watch on this socket without destroying it. */
    public fun detachAggregationWatch(watchId: String) {
        if (watchId.isEmpty()) return
        lock.withLock {
            attachedWatches.remove(watchId)
            sendMessage(buildJsonObject { put("action", "detach_aggregation_watch"); put("watchId", watchId) })
            maybeStartIdleTimerLocked()
        }
    }

    private fun hasAnyInterestLocked(): Boolean =
        pathRefs.isNotEmpty() || midsRefs > 0 || candleRefCoins.isNotEmpty() || oiRefCoins.isNotEmpty() ||
            chartHistoryWatches.isNotEmpty() || attachedWatches.isNotEmpty()

    private fun maybeStartIdleTimerLocked() {
        if (hasAnyInterestLocked() || idleDisconnectJob != null) return
        idleDisconnectJob = scope.launch {
            delay(IDLE_DISCONNECT_MS)
            if (!isActive) return@launch
            idleDisconnect()
        }
    }

    private fun idleDisconnect() {
        val shouldDrop = lock.withLock {
            idleDisconnectJob = null
            !hasAnyInterestLocked()
        }
        if (shouldDrop) disconnect()
    }

    private fun cancelIdleTimerLocked() {
        idleDisconnectJob?.cancel()
        idleDisconnectJob = null
    }

    // MARK: - Event streams

    /** A stream of all realm events. Each collector iterates independently. */
    public val events: Flow<RealmEvent>
        get() = bus.asSharedFlow()

    /** A stream of discriminated [TypedEvent] values. */
    public val typedEvents: Flow<TypedEvent>
        get() = filtered { TypedEvent.from(it) }

    /** A stream of connection status changes (replays the current value). */
    public val statusStream: StateFlow<ConnectionStatus>
        get() = statusFlow.asStateFlow()

    public fun operationEvents(): Flow<Pair<Operation, RealmEvent>> = filtered { event ->
        val op = event.operation
        if ((event.type == EventType.OPERATION_CREATED.wire || event.type == EventType.OPERATION_UPDATED.wire) && op != null) {
            op to event
        } else {
            null
        }
    }

    public fun balanceEvents(): Flow<Pair<String, RealmEvent>> = filtered { event ->
        val id = event.entityId
        if (event.type == EventType.BALANCE_UPDATED.wire && id != null) id to event else null
    }

    public fun exchangeEvents(): Flow<Pair<ExchangeState, RealmEvent>> = filtered { event ->
        val state = event.exchangeState
        if (event.type == EventType.EXCHANGE_UPDATED.wire && state != null) state to event else null
    }

    public fun exchangeNotifications(): Flow<RealmEvent> = filtered { event ->
        if (event.type == EventType.EXCHANGE_UPDATED.wire) event else null
    }

    public fun midsEvents(): Flow<Map<String, String>> = filtered { event ->
        val mids = event.mids
        if (event.type == EventType.MIDS_UPDATED.wire && mids != null) mids else null
    }

    public fun aggregationEvents(): Flow<Triple<String, PathAggregation?, RealmEvent>> = filtered { event ->
        val id = event.entityId
        if (event.type == EventType.AGGREGATION_UPDATED.wire && id != null) {
            Triple(id, event.aggregation, event)
        } else {
            null
        }
    }

    public fun twapEvents(): Flow<TypedEvent> = filtered { event ->
        when (event.type) {
            EventType.TWAP_STARTED.wire,
            EventType.TWAP_PROGRESS.wire,
            EventType.TWAP_COMPLETED.wire,
            EventType.TWAP_CANCELLED.wire,
            EventType.TWAP_FAILED.wire,
            -> TypedEvent.from(event)
            else -> null
        }
    }

    public fun chartSnapshotEvents(): Flow<Pair<String, RealmEvent>> = filtered { event ->
        val watchId = event.watchId
        if (event.type == EventType.CHART_SNAPSHOT_UPDATED.wire && watchId != null) watchId to event else null
    }

    public fun candleEvents(): Flow<CandleEvent> = filtered { event -> decodeCandleEvent(event, closedOnly = false) }

    public fun candleClosedEvents(): Flow<CandleEvent> = filtered { event -> decodeCandleEvent(event, closedOnly = true) }

    public fun oiEvents(): Flow<OIEvent> = filtered { event -> decodeOIEvent(event) }

    /**
     * A stream of projection delta frames: `object.valuation` events carrying a
     * path-keyed map of changed rows (plus optional removed paths) for a
     * projection watch. Frames for single-object watches (which carry a single
     * `valuation`) never appear here.
     */
    public fun projectionValuationEvents(): Flow<ProjectionValuationEvent> = filtered { event ->
        val watchId = event.watchId
        val valuations = event.valuations
        if (event.type == EventType.OBJECT_VALUATION.wire && watchId != null &&
            event.projection != null && valuations != null
        ) {
            ProjectionValuationEvent(watchId, valuations, event.removed, event)
        } else {
            null
        }
    }

    public fun objectValuationEvents(): Flow<ObjectValuationEvent> = filtered { event ->
        val valuation = event.valuation
        val path = event.path
        val watchId = event.watchId
        if (event.type == EventType.OBJECT_VALUATION.wire && valuation != null && path != null && watchId != null) {
            ObjectValuationEvent(valuation, path, watchId, event)
        } else {
            null
        }
    }

    public fun fillEvents(): Flow<Pair<SimFill, RealmEvent>> = filtered { event ->
        val fill = event.fill
        if (event.type == EventType.FILL_PREVIEWED.wire && fill != null) fill to event else null
    }

    public fun fillRecordedEvents(): Flow<Pair<Fill, RealmEvent>> = filtered { event ->
        val fill = event.recordedFill
        if (event.type == EventType.FILL_RECORDED.wire && fill != null) fill to event else null
    }

    public fun fundingEvents(): Flow<Pair<FundingPayment, RealmEvent>> = filtered { event ->
        val funding = event.funding
        if (event.type == EventType.EXCHANGE_FUNDING.wire && funding != null) funding to event else null
    }

    public fun typedExchangeEvents(): Flow<TypedEvent> = filtered { event ->
        when (val typed = TypedEvent.from(event)) {
            is TypedEvent.ExchangeUpdated,
            is TypedEvent.FillPreview,
            is TypedEvent.FillRecorded,
            is TypedEvent.FundingPaymentEvent,
            -> typed
            else -> null
        }
    }

    public fun typedFillEvents(): Flow<TypedEvent> = filtered { event ->
        when (val typed = TypedEvent.from(event)) {
            is TypedEvent.FillPreview, is TypedEvent.FillRecorded -> typed
            else -> null
        }
    }

    public fun typedFundingEvents(): Flow<TypedEvent> = filtered { event ->
        when (val typed = TypedEvent.from(event)) {
            is TypedEvent.FundingPaymentEvent -> typed
            else -> null
        }
    }

    /** A stream of resume events; each emission carries the hidden duration in seconds. */
    public val resumeStream: SharedFlow<Double>
        get() = resumeFlow.asSharedFlow()

    /** A stream that emits whenever the WebSocket completes authentication. */
    public val authenticatedStream: SharedFlow<Unit>
        get() = authenticatedFlow.asSharedFlow()

    /** A stream that emits whenever delivery moves to a new socket. See [onRotated]. */
    public val rotatedStream: SharedFlow<Unit>
        get() = rotatedFlow.asSharedFlow()

    private fun <T> filtered(transform: (RealmEvent) -> T?): Flow<T> =
        bus.asSharedFlow().mapNotNull(transform)

    private fun decodeCandleEvent(event: RealmEvent, closedOnly: Boolean): CandleEvent? {
        val matchesType = if (closedOnly) {
            event.type == EventType.CANDLE_CLOSED.wire
        } else {
            event.type == EventType.CANDLE_CLOSED.wire || event.type == EventType.CANDLE_UPDATED.wire
        }
        if (!matchesType) return null
        val market = event.market ?: return null
        val interval = event.interval?.let { CandleInterval.fromWire(it) } ?: return null
        val candle = event.candle ?: return null
        return CandleEvent(market, interval, candle)
    }

    private fun decodeOIEvent(event: RealmEvent): OIEvent? {
        if (event.type != EventType.OI_UPDATED.wire) return null
        val market = event.market ?: return null
        val interval = event.interval?.let { CandleInterval.fromWire(it) } ?: return null
        val bar = event.bar ?: return null
        return OIEvent(market, interval, bar, event.isClosed ?: false)
    }

    // MARK: - Gap detection + handlers

    private fun checkDeliveryGap(seq: Int) {
        if (lastDeliverySeq > 0 && seq > lastDeliverySeq + 1) {
            val missed = seq - lastDeliverySeq - 1
            log.warning(
                "websocket",
                metadata = mapOf(
                    "missed" to missed.toString(),
                    "previousSeq" to lastDeliverySeq.toString(),
                    "currentSeq" to seq.toString(),
                ),
            ) { "delivery gap detected" }
            gapHandlers.values.forEach { it(missed) }
        }
        lastDeliverySeq = seq
    }

    /**
     * Register a handler that fires when delivery loss is detected. The
     * handler receives the number of missed events.
     *
     * Fires for two kinds of loss: a hole the client observed in the
     * server-assigned deliverySeq (the count is exact), and a server-sent
     * `stream.resync` marker announcing that events were dropped before they
     * were sequenced — a loss no sequence check can see (the count is a floor
     * of 1). Both mean the same thing for recovery: refetch.
     */
    public fun onGap(handler: (Int) -> Unit): UUID {
        val id = UUID.randomUUID()
        gapHandlers[id] = handler
        return id
    }

    public fun removeGapHandler(id: UUID) {
        gapHandlers.remove(id)
    }

    public fun onResume(handler: (Double) -> Unit): UUID {
        val id = UUID.randomUUID()
        resumeHandlers[id] = handler
        return id
    }

    public fun removeResumeHandler(id: UUID) {
        resumeHandlers.remove(id)
    }

    public fun onAuthenticated(handler: () -> Unit): UUID {
        val id = UUID.randomUUID()
        authenticatedHandlers[id] = handler
        return id
    }

    public fun removeAuthenticatedHandler(id: UUID) {
        authenticatedHandlers.remove(id)
    }

    /**
     * Register a listener for delivery moving to a new socket without an outage
     * (see [rotateConnection]).
     *
     * This is not a reconnect: no status change is emitted, nothing was missed,
     * and there is no gap to recover. It exists for state the server holds
     * per-connection and therefore cannot survive the swap — a standalone
     * aggregation watch has to be re-created against the new socket, because the
     * old one died with the connection it was registered on. Anything the
     * manager re-issues itself (mids, candles, OI, path watches, chart-history
     * watches) is already handled and needs no hook.
     *
     * Do NOT use this to refetch history or run gap recovery; [onAuthenticated]
     * is the hook for that. Rotations are routine, so a refetch here multiplies
     * into steady background load across every connected client.
     */
    public fun onRotated(handler: () -> Unit): UUID {
        val id = UUID.randomUUID()
        rotatedHandlers[id] = handler
        return id
    }

    public fun removeRotatedHandler(id: UUID) {
        rotatedHandlers.remove(id)
    }

    // MARK: - App lifecycle

    private fun installLifecycleLocked() {
        if (lifecycleInstalled) return
        val bridge = lifecycleBridge ?: return
        bridge.install(
            onForeground = { handleAppWillEnterForeground() },
            onBackground = { handleAppDidEnterBackground() },
        )
        lifecycleInstalled = true
    }

    private fun removeLifecycleLocked() {
        if (!lifecycleInstalled) return
        lifecycleBridge?.uninstall()
        lifecycleInstalled = false
    }

    /** Host hook: notify the manager the app entered the background. */
    public fun handleAppDidEnterBackground() {
        lock.withLock { hiddenAtMs = System.currentTimeMillis() }
    }

    /** Host hook: notify the manager the app returned to the foreground. */
    public fun handleAppWillEnterForeground() {
        val hiddenDuration = lock.withLock {
            val hidden = hiddenAtMs ?: return
            hiddenAtMs = null
            (System.currentTimeMillis() - hidden) / 1000.0
        }
        if (hiddenDuration < RESUME_HIDDEN_THRESHOLD_S) return
        fireResume(hiddenDuration)
        probeStaleConnection()
    }

    /** Drive a resume signal directly (tests / manual recovery). */
    public fun triggerResume(hiddenDurationSeconds: Double) {
        fireResume(hiddenDurationSeconds)
        probeStaleConnection()
    }

    private fun fireResume(hiddenDuration: Double) {
        resumeHandlers.values.forEach { it(hiddenDuration) }
        resumeFlow.tryEmit(hiddenDuration)
    }

    private fun probeStaleConnection() {
        lock.withLock {
            resumeProbeJob?.cancel()
            if (webSocket == null || statusFlow.value != ConnectionStatus.CONNECTED) return
            val baseline = lastMessageAtMs
            sendMessage(buildJsonObject { put("action", "ping") })
            resumeProbeJob = scope.launch {
                delay(RESUME_PING_TIMEOUT_MS)
                if (!isActive) return@launch
                checkResumeProbe(baseline)
            }
        }
    }

    private fun checkResumeProbe(baseline: Long) {
        lock.withLock {
            resumeProbeJob = null
            if (lastMessageAtMs != baseline) return
            log.warning("websocket") { "resume probe timeout, forcing reconnect" }
            stopHeartbeatLocked()
            webSocket?.cancel()
            webSocket = null
            dropLiveSocketLocked()
        }
    }

    // MARK: - Connection internals

    private fun ensureConnectedLocked() {
        if (webSocket != null) return
        shouldReconnect = true
        installLifecycleLocked()
        doConnectLocked()
    }

    private fun doConnectLocked() {
        cancelRotationLocked()
        // A warming replacement for a socket we are about to replace outright
        // is moot; the connect below supersedes it.
        abortHandoffLocked()

        val existing = webSocket
        webSocket = null
        existing?.cancel()

        setStatusLocked(ConnectionStatus.CONNECTING)
        log.debug("websocket", metadata = mapOf("url" to wsUrl.toString(), "realmId" to realmId)) { "connecting" }

        val ws = openSocketLocked()
        webSocket = ws
        authenticateLocked(ws)
    }

    private fun openSocketLocked(): WebSocket {
        val request = Request.Builder().url(wsUrl).build()
        return socketFactory.newWebSocket(request, SocketListener())
    }

    private fun authenticateLocked(target: WebSocket) {
        val gt = getToken
        if (gt != null) {
            scope.launch {
                try {
                    val fresh = gt()
                    lock.withLock { token = fresh }
                    sendMessage(target, authMsg(fresh))
                } catch (e: Throwable) {
                    log.error("websocket", e) { "token refresh failed on reconnect, falling back to cached token" }
                    sendMessage(target, authMsg(token))
                }
            }
        } else {
            sendMessage(target, authMsg(token))
        }
    }

    private fun handleMessage(text: String) {
        lastMessageAtMs = System.currentTimeMillis()
        val obj = runCatching { arcaJson.parseToJsonElement(text).jsonObject }.getOrNull()

        if (obj != null) {
            when (obj["type"]?.jsonPrimitive?.contentOrNull ?: "") {
                "pong" -> return
                "stream.resync" -> {
                    // The server announced that events for this connection
                    // were dropped BEFORE they were sequenced (delivery-queue
                    // overflow under backpressure), so no deliverySeq gap will
                    // ever reveal the loss — this marker is the only signal.
                    // Run the same recovery as a detected gap; the count is a
                    // floor of 1 (the server knows events were lost, not how
                    // many). The marker carries its own deliverySeq, so the
                    // sequence check runs first and stays contiguous for
                    // subsequent messages. A control message: never emitted to
                    // event flows.
                    obj["deliverySeq"]?.jsonPrimitive?.intOrNull?.let { lock.withLock { checkDeliveryGap(it) } }
                    log.warning("websocket") { "server announced event loss (stream.resync)" }
                    gapHandlers.values.forEach { it(1) }
                    return
                }
                "authenticated" -> { handleAuthenticated(obj); return }
                "projection_watch_created" -> { handleProjectionWatchCreated(obj); return }
                "error" -> { handleServerError(obj); return }
                "mids.snapshot" -> {
                    val midsRaw = obj["mids"]?.jsonObject ?: return
                    val mids = midsRaw.mapValues { it.value.jsonPrimitive.content }
                    emit(RealmEvent(type = EventType.MIDS_UPDATED.wire, mids = mids))
                    return
                }
                "candles.updated" -> {
                    val items = obj["candles"]?.jsonArray ?: return
                    obj["deliverySeq"]?.jsonPrimitive?.intOrNull?.let { lock.withLock { checkDeliveryGap(it) } }
                    for (item in items) {
                        val io = item.jsonObject
                        val market = io["market"]?.jsonPrimitive?.contentOrNull ?: continue
                        val interval = io["interval"]?.jsonPrimitive?.contentOrNull ?: continue
                        val candleEl = io["candle"] ?: continue
                        val candle = runCatching {
                            arcaJson.decodeFromJsonElement(Candle.serializer(), candleEl)
                        }.getOrNull() ?: continue
                        emit(
                            RealmEvent(
                                type = EventType.CANDLE_UPDATED.wire,
                                market = market,
                                interval = interval,
                                candle = candle,
                            ),
                        )
                    }
                    return
                }
                "watch_snapshot" -> {
                    handleWatchSnapshot(obj)
                    // falls through to gap check + generic decode, matching Swift
                }
            }

            obj["deliverySeq"]?.jsonPrimitive?.intOrNull?.let { lock.withLock { checkDeliveryGap(it) } }
        }

        runCatching { arcaJson.decodeFromString(RealmEvent.serializer(), text) }.getOrNull()?.let { emit(it) }
    }

    private fun handleAuthenticated(obj: JsonObject?) {
        lock.withLock {
            log.info("websocket") { "authenticated" }
            reconnectAttempt = 0
            lastDeliverySeq = 0
            if (obj != null) readServerLifetimeLocked(obj)
            setStatusLocked(ConnectionStatus.CONNECTED)
            startHeartbeatLocked()
            resubscribeAllLocked(webSocket)
            scheduleRotationLocked()
        }
        // Notify subscribers AFTER all subscriptions are re-issued.
        authenticatedHandlers.values.forEach { it() }
        authenticatedFlow.tryEmit(Unit)
    }

    private fun resubscribeAllLocked(target: WebSocket?) {
        subscribedMids?.let { sendMessage(target, authlessSubscribeMids(it.first, it.second)) }
        subscribedCandles?.let { sendMessage(target, subscribeCandlesMsg(it.first, it.second.map { iv -> iv.wire })) }
        subscribedOI?.let { sendMessage(target, subscribeOIMsg(it.first, it.second.map { iv -> iv.wire })) }
        if (midsRefs > 0 && subscribedMids == null) {
            sendMessage(target, authlessSubscribeMids(midsExchange, emptyList()))
        }
        if (candleRefCoins.isNotEmpty() && subscribedCandles == null) {
            syncCandleSubscriptionLocked(target)
        }
        if (oiRefCoins.isNotEmpty() && subscribedOI == null) {
            syncOISubscriptionLocked(target)
        }
        pathRefs.keys.forEach { path ->
            sendMessage(target, buildJsonObject { put("action", "watch"); put("path", path) })
        }
        chartHistoryWatches.forEach { (watchId, req) ->
            sendMessage(target, watchChartHistoryMsg(watchId, req.target, req.kind, req.objectId))
        }
        // Re-register REST-created watches. The registry is per-pod, so a
        // reconnect landing elsewhere answers "unknown watch" — the watch is
        // genuinely gone there and the stream recreates it.
        attachedWatches.forEach { watchId ->
            sendMessage(target, attachAggregationWatchMsg(watchId))
        }
        // Projection watch requests parked while disconnected go out now that
        // auth completed; the replies resolve the callers' pending requests.
        if (pendingProjectionSends.isNotEmpty()) {
            pendingProjectionSends.forEach { (requestId, projection) ->
                sendMessage(target, watchProjectionMsg(projection, requestId))
            }
            pendingProjectionSends.clear()
        }
    }

    private fun handleServerError(obj: JsonObject) {
        val message = obj["message"]?.jsonPrimitive?.contentOrNull ?: "Unknown WebSocket error"

        // An error carrying a requestId is scoped to that request (e.g. a
        // watch_projection the caller isn't authorized for): reject only the
        // matching pending request and leave the connection serving.
        val requestId = obj["requestId"]?.jsonPrimitive?.contentOrNull
        if (requestId != null) {
            val deferred = lock.withLock {
                pendingProjectionSends.remove(requestId)
                pendingProjectionRequests.remove(requestId)
            }
            if (deferred != null) {
                log.warning("websocket", metadata = mapOf("message" to message, "requestId" to requestId)) {
                    "projection watch request rejected"
                }
                deferred.completeExceptionally(ArcaException.Unknown("WS_REQUEST_ERROR", message))
                return
            }
        }

        log.error("websocket", metadata = mapOf("message" to message)) { "server error" }
        lock.withLock {
            webSocket?.cancel()
            webSocket = null
            dropLiveSocketLocked()
        }
    }

    private fun handleProjectionWatchCreated(obj: JsonObject) {
        val requestId = obj["requestId"]?.jsonPrimitive?.contentOrNull ?: return
        val deferred = lock.withLock { pendingProjectionRequests.remove(requestId) } ?: return
        val valuations = obj["valuations"]?.let { el ->
            runCatching {
                arcaJson.decodeFromJsonElement(
                    kotlinx.serialization.builtins.ListSerializer(ProjectedValuation.serializer()),
                    el,
                )
            }.getOrNull()
        } ?: emptyList()
        deferred.complete(
            ProjectionWatchCreated(
                watchId = obj["watchId"]?.jsonPrimitive?.contentOrNull ?: "",
                projection = obj["projection"]?.jsonPrimitive?.contentOrNull ?: "",
                fields = obj["fields"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList(),
                valuations = valuations,
                cursor = obj["cursor"]?.jsonPrimitive?.contentOrNull,
            ),
        )
    }

    private fun handleWatchSnapshot(obj: JsonObject) {
        val watchId = obj["watchId"]?.jsonPrimitive?.contentOrNull ?: return
        obj["valuation"]?.let { valEl ->
            val path = obj["path"]?.jsonPrimitive?.contentOrNull
            val valuation = runCatching { arcaJson.decodeFromJsonElement(ObjectValuation.serializer(), valEl) }.getOrNull()
            if (path != null && valuation != null) {
                emit(RealmEvent(type = EventType.OBJECT_VALUATION.wire, valuation = valuation, path = path, watchId = watchId))
            }
        }
        (obj["valuations"] as? JsonObject)?.let { vals ->
            for ((objPath, valEl) in vals) {
                val valuation = runCatching { arcaJson.decodeFromJsonElement(ObjectValuation.serializer(), valEl) }.getOrNull()
                if (valuation != null) {
                    emit(RealmEvent(type = EventType.OBJECT_VALUATION.wire, valuation = valuation, path = objPath, watchId = watchId))
                }
            }
        }
    }

    private fun emit(event: RealmEvent) {
        bus.tryEmit(event)
    }

    // MARK: - Reconnection

    private fun scheduleReconnectLocked() {
        if (reconnectJob != null) return
        val delaySeconds = min(2.0.pow(reconnectAttempt.toDouble()), maxReconnectDelaySeconds)
        reconnectAttempt += 1
        log.warning(
            "websocket",
            metadata = mapOf("attempt" to reconnectAttempt.toString(), "delaySeconds" to String.format("%.1f", delaySeconds)),
        ) { "scheduling reconnect" }
        reconnectJob = scope.launch {
            delay((delaySeconds * 1000).toLong())
            if (!isActive) return@launch
            lock.withLock {
                reconnectJob = null
                doConnectLocked()
            }
        }
    }

    // MARK: - Heartbeat

    private fun startHeartbeatLocked() {
        stopHeartbeatLocked()
        lastMessageAtMs = System.currentTimeMillis()
        pingJob = scope.launch {
            while (isActive) {
                delay(PING_INTERVAL_MS)
                if (!isActive) return@launch
                heartbeatTick()
            }
        }
    }

    private fun stopHeartbeatLocked() {
        pingJob?.cancel()
        pingJob = null
    }

    private fun heartbeatTick() {
        lock.withLock {
            val elapsed = (System.currentTimeMillis() - lastMessageAtMs) / 1000.0
            if (elapsed >= STALE_THRESHOLD_S) {
                log.warning(
                    "websocket",
                    metadata = mapOf("elapsedSeconds" to String.format("%.1f", elapsed), "thresholdSeconds" to String.format("%.1f", STALE_THRESHOLD_S)),
                ) { "connection stale, forcing reconnect" }
                stopHeartbeatLocked()
                webSocket?.cancel()
                webSocket = null
                dropLiveSocketLocked()
                return
            }
            sendMessage(buildJsonObject { put("action", "ping") })
        }
    }

    // MARK: - Gapless rotation

    /**
     * Replace the current socket with a fresh one without interrupting delivery.
     *
     * The replacement authenticates and re-issues every subscription while the
     * current socket keeps streaming. Only once the server confirms those
     * subscriptions are live does it take over, and only then does the old
     * socket close — so there is no window in which nothing is subscribed. A
     * failure anywhere along the way leaves the current socket untouched and
     * serving, which makes the worst case "nothing happened".
     *
     * Returns false when there is no healthy socket to hand off from, or when a
     * handoff is already under way.
     */
    public fun rotateConnection(): Boolean = lock.withLock {
        if (!shouldReconnect) return@withLock false
        if (handoffWebSocket != null) return@withLock false
        if (statusFlow.value != ConnectionStatus.CONNECTED) return@withLock false
        if (webSocket == null) return@withLock false

        log.debug("websocket") { "warming replacement socket" }
        // Armed before the socket exists so a half-built one can never be left
        // hanging around unnoticed.
        armHandoffTimeoutLocked()
        val ws = openSocketLocked()
        handoffWebSocket = ws
        authenticateLocked(ws)
        true
    }

    /**
     * Handle a frame that arrived on a socket still warming up beside the live
     * one. Only the handshake matters here — see [handleWarmingAuthenticated]
     * and [promoteHandoff].
     */
    private fun handleWarmingMessage(socket: WebSocket, text: String) {
        val obj = runCatching { arcaJson.parseToJsonElement(text).jsonObject }.getOrNull() ?: return
        when (obj["type"]?.jsonPrimitive?.contentOrNull ?: "") {
            "error" -> {
                val message = obj["message"]?.jsonPrimitive?.contentOrNull ?: "Unknown WebSocket error"
                log.warning("websocket", metadata = mapOf("message" to message)) { "handoff rejected by server" }
                lock.withLock {
                    abortHandoffLocked()
                    scheduleRotationLocked(HANDOFF_RETRY_MS)
                }
            }
            "authenticated" -> handleWarmingAuthenticated(socket, obj)
            "pong" -> promoteHandoff(socket)
            // The live socket is carrying this same stream, so anything arriving
            // here before the takeover duplicates something consumers already
            // have. Dropping it avoids a double dispatch and keeps gap detection
            // on a single sequence space.
            else -> Unit
        }
    }

    private fun handleWarmingAuthenticated(socket: WebSocket, obj: JsonObject) {
        lock.withLock {
            if (handoffWebSocket !== socket) return
            readServerLifetimeLocked(obj)
            resubscribeAllLocked(socket)
            // Queued behind the batch above; its reply is the barrier this
            // socket takes over on.
            sendMessage(socket, buildJsonObject { put("action", "ping") })
        }
    }

    /**
     * Hand delivery over to the warmed socket and retire the current one.
     *
     * The server reads one connection's messages in order, so a reply to the
     * ping queued behind the resubscribe batch proves every subscription in that
     * batch is registered — from here on, live broadcasts reach this socket.
     * That is what makes this the point where it can take over without leaving a
     * gap.
     *
     * Any snapshot those subscriptions trigger is sent asynchronously and may
     * well arrive after the pong, so it is not part of the barrier — and it is
     * not needed, because the socket being retired has been delivering the same
     * stream right up to this moment, leaving consumer state current at the swap.
     */
    private fun promoteHandoff(socket: WebSocket) {
        val retired = lock.withLock {
            if (handoffWebSocket !== socket) return
            handoffWebSocket = null
            cancelHandoffTimeoutLocked()

            val previous = webSocket
            // Installing the replacement first is what silences the outgoing
            // socket: the listener routes purely by identity, so from here its
            // buffered frames, its failure and its close are all ignored — no
            // second dispatch to consumers, no spurious DISCONNECTED, and no
            // reconnect competing with the socket that just took over.
            webSocket = socket
            // New connection, new sequence space.
            lastDeliverySeq = 0
            lastMessageAtMs = System.currentTimeMillis()
            startHeartbeatLocked()
            scheduleRotationLocked()
            previous
        }
        if (retired !== socket) retired?.cancel()
        log.info("websocket") { "rotated onto replacement socket" }

        // Status deliberately does not move. Delivery never stopped, so emitting
        // DISCONNECTED would put consumers into a reconnecting state and run gap
        // recovery for a gap that did not happen. State the swap genuinely
        // cannot carry over is re-established via the rotated handlers instead.
        rotatedHandlers.values.forEach { it() }
        rotatedFlow.tryEmit(Unit)
    }

    /** Abandon a warming socket. The live socket is left exactly as it was. */
    private fun abortHandoffLocked() {
        cancelHandoffTimeoutLocked()
        val ws = handoffWebSocket ?: return
        handoffWebSocket = null
        ws.cancel()
    }

    /**
     * React to a warming socket dying before it took over. Returns false when
     * [socket] was not the warming one, leaving the caller to handle it as the
     * live socket.
     */
    private fun handleWarmingSocketLoss(socket: WebSocket): Boolean = lock.withLock {
        if (handoffWebSocket !== socket) return@withLock false
        // The live socket never stopped serving, so consumers see nothing; try
        // again later rather than escalating to the reconnect path.
        handoffWebSocket = null
        cancelHandoffTimeoutLocked()
        scheduleRotationLocked(HANDOFF_RETRY_MS)
        true
    }

    private fun armHandoffTimeoutLocked() {
        cancelHandoffTimeoutLocked()
        handoffTimeoutJob = scope.launch {
            delay(handoffTimeoutMs)
            if (!isActive) return@launch
            lock.withLock {
                handoffTimeoutJob = null
                if (handoffWebSocket == null) return@withLock
                log.warning("websocket") { "handoff timed out, abandoning replacement socket" }
                abortHandoffLocked()
                scheduleRotationLocked(HANDOFF_RETRY_MS)
            }
        }
    }

    private fun cancelHandoffTimeoutLocked() {
        handoffTimeoutJob?.cancel()
        handoffTimeoutJob = null
    }

    /**
     * Arm the next rotation. [delayMs] overrides the schedule, which is how a
     * failed handoff retries before the lifetime it is racing runs out.
     */
    private fun scheduleRotationLocked(delayMs: Long? = null) {
        cancelRotationLocked()
        // A configured 0 is an opt-out and outranks the server's figure. The
        // server reports a real constraint, so it wins over any other configured
        // value — but it must not resurrect rotation for a caller who turned it
        // off, or the documented escape hatch would be inoperative on the one
        // fleet that advertises a cap.
        val configured = connectionLifetimeMs ?: DEFAULT_CONNECTION_LIFETIME_MS
        val lifetime = if (configured == 0L) 0L else serverLifetimeMs ?: configured
        if (delayMs == null && lifetime <= 0) return
        val wait = delayMs ?: run {
            val base = lifetime * ROTATE_AT
            val spread = base * ROTATE_JITTER
            (base - spread + Random.nextDouble() * spread * 2).toLong()
        }
        rotationJob = scope.launch {
            delay(wait)
            if (!isActive) return@launch
            lock.withLock { rotationJob = null }
            rotateConnection()
        }
    }

    private fun cancelRotationLocked() {
        rotationJob?.cancel()
        rotationJob = null
    }

    /**
     * Adopt the socket lifetime the server reports at auth.
     *
     * The server sits behind the proxy that enforces the cap, so it is the only
     * party that knows the real figure. Taking it from the wire means retuning
     * the cap is a server config change rather than an SDK release — otherwise
     * every deployed client keeps rotating against a number that silently went
     * stale.
     */
    private fun readServerLifetimeLocked(obj: JsonObject) {
        val sec = (obj["maxConnectionLifetimeSec"] as? JsonPrimitive)?.doubleOrNull ?: return
        if (sec > 0 && sec.isFinite()) serverLifetimeMs = (sec * 1000).toLong()
    }

    // MARK: - Messaging + status

    private fun sendMessage(message: JsonObject) {
        sendMessage(webSocket, message)
    }

    private fun sendMessage(target: WebSocket?, message: JsonObject) {
        val ws = target ?: return
        val sent = ws.send(message.toString())
        if (!sent) {
            log.debug("websocket") { "message not enqueued (socket closing)" }
        }
    }

    private fun setStatusLocked(newStatus: ConnectionStatus) {
        if (newStatus == statusFlow.value) return
        log.debug("websocket", metadata = mapOf("from" to statusFlow.value.name, "to" to newStatus.name)) { "status" }
        statusFlow.value = newStatus
    }

    /** Inject a raw WebSocket message for testing. Not for production use. */
    internal fun injectMessage(text: String) {
        handleMessage(text)
    }

    /** Cancel the manager's background scope. Called by [Arca] on shutdown. */
    internal fun shutdown() {
        disconnect()
        scope.cancel()
    }

    // MARK: - Message builders

    private fun authMsg(token: String): JsonObject = buildJsonObject {
        put("action", "auth")
        put("token", token)
        put("realmId", realmId)
        put("capabilities", buildJsonArray { ArcaClient.ADVERTISED_CAPABILITIES.forEach { add(it) } })
    }

    private fun authlessSubscribeMids(exchange: String, coins: List<String>): JsonObject = buildJsonObject {
        put("action", "subscribe_mids")
        put("exchange", exchange)
        put("coins", buildJsonArray { coins.forEach { add(it) } })
    }

    private fun subscribeCandlesMsg(coins: List<String>, intervals: List<String>): JsonObject = buildJsonObject {
        put("action", "subscribe_candles")
        put("coins", buildJsonArray { coins.forEach { add(it) } })
        put("intervals", buildJsonArray { intervals.forEach { add(it) } })
        put("batch", true)
    }

    private fun subscribeOIMsg(coins: List<String>, intervals: List<String>): JsonObject = buildJsonObject {
        put("action", "subscribe_oi")
        put("coins", buildJsonArray { coins.forEach { add(it) } })
        put("intervals", buildJsonArray { intervals.forEach { add(it) } })
    }

    private fun watchProjectionMsg(projection: String, requestId: String): JsonObject = buildJsonObject {
        put("action", "watch_projection")
        put("projection", projection)
        put("requestId", requestId)
    }

    private fun attachAggregationWatchMsg(watchId: String): JsonObject = buildJsonObject {
        put("action", "attach_aggregation_watch")
        put("watchId", watchId)
    }

    private fun watchChartHistoryMsg(watchId: String, target: String, kind: String, objectId: String?): JsonObject =
        buildJsonObject {
            put("action", "watch_chart_history")
            put("watchId", watchId)
            put("target", target)
            put("kind", kind)
            if (objectId != null) put("objectId", objectId)
        }

    // MARK: - OkHttp listener

    /**
     * Identity, not arrival order, decides what a frame means: the live socket
     * carries delivery, a socket still warming up carries only its own handshake
     * (see [handleWarmingMessage]), and a socket that is neither has already been
     * retired and is ignored outright.
     */
    private inner class SocketListener : WebSocketListener() {
        override fun onMessage(webSocket: WebSocket, text: String) {
            route(webSocket, text)
        }

        override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
            route(webSocket, bytes.utf8())
        }

        override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
            if (handleWarmingSocketLoss(webSocket)) {
                log.debug("websocket", t) { "replacement socket failed before takeover" }
                return
            }
            lock.withLock {
                if (this@WebSocketManager.webSocket !== webSocket) return
                log.warning("websocket", t) { "receive loop error" }
                this@WebSocketManager.webSocket = null
                dropLiveSocketLocked()
            }
        }

        override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
            if (handleWarmingSocketLoss(webSocket)) {
                log.debug("websocket") { "replacement socket closed before takeover" }
                return
            }
            lock.withLock {
                if (this@WebSocketManager.webSocket !== webSocket) return
                this@WebSocketManager.webSocket = null
                dropLiveSocketLocked()
            }
        }

        private fun route(socket: WebSocket, text: String) {
            when {
                this@WebSocketManager.webSocket === socket -> handleMessage(text)
                handoffWebSocket === socket -> handleWarmingMessage(socket, text)
            }
        }
    }

    /**
     * Common tail for the live socket going away: a warming replacement for it is
     * moot, because the reconnect path supersedes it.
     */
    private fun dropLiveSocketLocked() {
        cancelRotationLocked()
        abortHandoffLocked()
        setStatusLocked(ConnectionStatus.DISCONNECTED)
        if (shouldReconnect) scheduleReconnectLocked()
    }

    private companion object {
        const val UNSUB_DEBOUNCE_MS = 100L
        const val IDLE_DISCONNECT_MS = 60_000L
        const val PING_INTERVAL_MS = 30_000L
        const val STALE_THRESHOLD_S = 45.0
        const val RESUME_HIDDEN_THRESHOLD_S = 5.0
        const val RESUME_PING_TIMEOUT_MS = 2_000L

        // Default socket lifetime before a rotation. Sits below the 3600s cap
        // the production load balancer imposes, leaving room for the retries
        // below to land before the cap is reached.
        const val DEFAULT_CONNECTION_LIFETIME_MS = 50 * 60_000L

        // Fraction of the known lifetime at which to rotate.
        const val ROTATE_AT = 0.85

        // Rotations are spread out by ±this fraction. Every rotation costs a
        // resubscribe, and a resubscribe costs the server a full mids snapshot —
        // so a fleet that rotated on a shared schedule would arrive as a
        // thundering herd. The jitter is what keeps the cost flat instead of
        // spiky.
        const val ROTATE_JITTER = 0.1

        // A warming socket that has not taken over within this budget is
        // abandoned.
        const val HANDOFF_TIMEOUT_MS = 10_000L

        // Retry delay after a failed handoff.
        const val HANDOFF_RETRY_MS = 60_000L

        // Budget for the server to answer a watch_projection request.
        const val PROJECTION_REQUEST_TIMEOUT_MS = 10_000L
    }
}

/** The server's reply to a `watch_projection` request. */
public data class ProjectionWatchCreated(
    public val watchId: String,
    public val projection: String,
    /** The projection's registered field set at watch-creation time. */
    public val fields: List<String>,
    /** First page of the initial snapshot. */
    public val valuations: List<ProjectedValuation>,
    /** Set when the snapshot has more pages; fetch the rest via REST. */
    public val cursor: String?,
)

/** Projection delta-frame payload: changed rows, removed paths, and the raw event. */
public data class ProjectionValuationEvent(
    public val watchId: String,
    /** Changed rows keyed by object path — merge into local state by path. */
    public val valuations: Map<String, ProjectedValuation>,
    /** Paths deleted since the last frame — drop these rows. */
    public val removed: List<String>?,
    public val event: RealmEvent,
)

/** Object-valuation event payload: a valuation plus its path, watch id, and raw event. */
public data class ObjectValuationEvent(
    public val valuation: ObjectValuation,
    public val path: String,
    public val watchId: String,
    public val event: RealmEvent,
)
