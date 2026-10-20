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
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.JsonArray
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
import network.arca.sdk.models.executionFill
import network.arca.sdk.models.AggregationSource
import network.arca.sdk.models.AggregationSourceType
import network.arca.sdk.models.Candle
import network.arca.sdk.models.CandleEvent
import network.arca.sdk.models.CandleInterval
import network.arca.sdk.models.OIEvent
import network.arca.sdk.models.ConnectionStatus
import network.arca.sdk.models.CreateWatchResponse
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
    private var handoffAuthenticated = false

    private var subscribedMids: Pair<String, List<String>>? = null
    private var subscribedCandles: Pair<List<String>, List<CandleInterval>>? = null
    private var subscribedOI: Pair<List<String>, List<CandleInterval>>? = null
    private var shouldReconnect = false
    private var reconnectAttempt = 0
    private var reconnectJob: Job? = null
    private var stableJob: Job? = null
    private var rotationJob: Job? = null
    private var handoffTimeoutJob: Job? = null
    private var serverLifetimeMs: Long? = null

    private val pathRefs = HashMap<String, Int>()
    private var midsRefs = 0
    // Consumers holding an all-markets interest (acquireMids(allMarkets =
    // true)). While non-zero the Arca socket subscribes to every market;
    // otherwise it subscribes to the union of declared price interests.
    private var midsAllRefs = 0
    private var midsExchange = "sim"

    // Latest merged mid prices, held while the mids subscription is live.
    //
    // The server answers each `subscribe_mids` with one `mids.snapshot`, and
    // the subscription is ref-counted, so only the consumer that took the 0→1
    // edge is collecting when that snapshot arrives. Without a retained copy
    // every later `watchPrices()` starts empty and fills one delta at a time —
    // so a market that does not tick stays unpriced for that consumer for as
    // long as it stays quiet. [midsEvents] replays this map to each new
    // collector as its first element.
    private var retainedMids: Map<String, String>? = null
        set(value) {
            field = value
            // Conflate full merged snapshots, never sparse deltas. Clearing
            // retention also clears replay for a later subscription lifetime.
            midsSnapshots.value = MidsSnapshot(midsSnapshots.value.revision + 1, value)
        }
    private data class MidsSnapshot(val revision: Long, val prices: Map<String, String>?)
    private val midsSnapshots = MutableStateFlow(MidsSnapshot(0, null))
    private val pendingPublicPrices = mutableMapOf<String, String>()
    private var publicFlushJob: Job? = null
    private var lastPublicFlushNs = 0L
    private var publicBatchEpoch = 0L
    private fun clearPublicPrices() { ++publicBatchEpoch; publicFlushJob?.cancel(); publicFlushJob = null; pendingPublicPrices.clear() }
    private fun queuePublicPrices(mids: Map<String, String>) {
        mids.forEach { (market, price) ->
            if (price != (pendingPublicPrices[market] ?: retainedMids?.get(market))) pendingPublicPrices[market] = price
        }
        if (pendingPublicPrices.isEmpty()) return
        val waitNs = 100_000_000L - (System.nanoTime() - lastPublicFlushNs)
        if (waitNs <= 0) flushPublicPrices()
        else if (publicFlushJob == null) {
            val epoch = publicBatchEpoch
            publicFlushJob = scope.launch {
                delay((waitNs + 999_999) / 1_000_000)
                lock.withLock { if (epoch == publicBatchEpoch) flushPublicPrices() }
            }
        }
    }
    internal fun flushPublicPrices() = lock.withLock {
        publicFlushJob?.cancel(); publicFlushJob = null
        ++publicBatchEpoch
        val mids = pendingPublicPrices.filter { (market, price) -> retainedMids?.get(market) != price }; pendingPublicPrices.clear()
        if (mids.isNotEmpty()) {
            lastPublicFlushNs = System.nanoTime()
            retainedMids = (retainedMids ?: emptyMap()) + mids
            marketDataRecorder.prices(mids, true, marketDataRouter.priceInterestMarkets)
            publishMarketDiagnostics()
            emit(RealmEvent(type = "mids.updated", mids = mids))
        }
    }
    private val marketDataRecorder = MarketDataDiagnosticsRecorder()
    private val diagnosticUpdates = MutableStateFlow(MarketDataDiagnostics())
    private var lastDiagnosticNs = 0L
    public val marketDataDiagnostics: MarketDataDiagnostics get() = lock.withLock { marketDataRecorder.snapshot(marketDataRouter) }
    public fun watchMarketDataDiagnostics(): StateFlow<MarketDataDiagnostics> = lock.withLock {
        diagnosticUpdates.value = marketDataRecorder.snapshot(marketDataRouter)
        diagnosticUpdates.asStateFlow()
    }
    private fun publishMarketDiagnostics(force: Boolean = false) {
        if (diagnosticUpdates.subscriptionCount.value == 0) return
        val now = System.nanoTime()
        if (!force && now - lastDiagnosticNs < 1_000_000_000) return
        lastDiagnosticNs = now
        diagnosticUpdates.value = marketDataRecorder.snapshot(marketDataRouter)
    }
    private val marketDataRouter = MarketDataRouter()
    private var publicSource: PublicMarketSource? = null
    private var publicSourceEpoch = 0L
    private var publicConnectionEpoch = 0L
    private var marketDataConfigurationEpoch = 0L
    private var publicNetwork = HyperliquidNetwork.MAINNET
    private var publicSuspended = false
    internal var publicSourceFactory: (String, (Long, PublicMarketUpdate) -> Unit) -> PublicMarketSource =
        { url, receive -> HyperliquidMarketSource(url, receive) }

    internal fun beginMarketDataConfiguration(): Long = lock.withLock {
        stopPublicSourceLocked()
        marketDataRouter.configure(MarketDataPreference.ARCA, emptyList())
        ++marketDataConfigurationEpoch
    }
    internal fun configureMarketData(epoch: Long, preference: MarketDataPreference, network: HyperliquidNetwork, markets: Collection<network.arca.sdk.models.Market>) = lock.withLock {
        if (epoch != marketDataConfigurationEpoch) return@withLock
        publicNetwork = network
        marketDataRouter.configure(preference, markets)
        syncPublicSourceLocked()
    }
    internal fun setMarketDataBudget(budget: MarketDataBudget?) = lock.withLock {
        marketDataRouter.setBudget(budget); syncPublicSourceLocked()
    }
    public val marketDataSourceStatus: MarketDataSourceStatus get() = lock.withLock { marketDataRouter.status() }
    internal fun registerPriceMarkets(): String = lock.withLock {
        UUID.randomUUID().toString().also { marketDataRouter.register(it) }
    }
    internal fun updatePriceMarkets(owner: String, markets: Set<String>, revision: Long = 0) = lock.withLock {
        marketDataRouter.update(owner, markets, revision); syncPublicSourceLocked()
    }
    /**
     * Dropping an owner can only narrow the Arca subscription, so the socket
     * side waits out the release debounce like [releaseMids] does; the direct
     * provider diffs its own subscriptions at once.
     */
    internal fun releasePriceMarkets(owner: String) = lock.withLock {
        marketDataRouter.release(owner); syncPublicSourceLocked(arca = false); scheduleMidsNarrowingLocked()
    }
    private fun stopPublicSourceLocked() {
        clearPublicPrices(); lastPublicFlushNs = 0
        ++publicSourceEpoch; publicConnectionEpoch = 0
        publicSource?.close(); publicSource = null
        marketDataRouter.unavailable(null)
        marketDataRecorder.suspend()
        publishMarketDiagnostics(true)
    }
    /**
     * Pushes the current interest set to both providers: the direct
     * Hyperliquid source (per-coin subscriptions) and, unless `arca` is
     * false, the Arca socket (`subscribe_mids` with the union of declared
     * markets). Every interest change passes through here.
     */
    private fun syncPublicSourceLocked(arca: Boolean = true) {
        if (arca) syncArcaMidsLocked()
        marketDataRecorder.retain(marketDataRouter.priceInterestMarkets)
        val subscriptions = marketDataRouter.subscriptions
        pendingPublicPrices.keys.retainAll(marketDataRouter.directCoverageMarkets)
        if (publicSuspended || subscriptions.isEmpty()) { stopPublicSourceLocked(); return }
        if (publicSource == null) {
            val epoch = ++publicSourceEpoch
            publicSource = publicSourceFactory(publicNetwork.websocketUrl) { connection, update ->
                lock.withLock {
                    if (epoch != publicSourceEpoch || connection < publicConnectionEpoch) return@withLock
                    publicConnectionEpoch = connection
                    if (update is PublicMarketUpdate.Traffic) {
                        marketDataRecorder.bytes(update.bytes, true); publishMarketDiagnostics(); return@withLock
                    }
                    if (update is PublicMarketUpdate.Unavailable) {
                        marketDataRecorder.failure()
                        clearPublicPrices()
                        log.warning("market-data") { update.reason }
                    }
                    val before = marketDataRouter.subscriptions
                    marketDataRouter.direct(update, System.currentTimeMillis(), System.nanoTime() / 1_000_000)?.let { event ->
                        marketDataRecorder.recovered()
                        if (event.mids != null) queuePublicPrices(event.mids) else { marketDataRecorder.candle(true); emit(event) }
                    }
                    if (before != marketDataRouter.subscriptions) syncPublicSourceLocked(arca = false)
                    publishMarketDiagnostics(update is PublicMarketUpdate.Unavailable)
                }
            }
        }
        publicSource?.subscribe(subscriptions)
        publishMarketDiagnostics(true)
    }
    private fun emitArcaMarketEvent(event: RealmEvent, snapshot: Boolean = false) = lock.withLock {
        if (event.type == EventType.MIDS_UPDATED.wire && event.mids != null) {
            val mids = marketDataRouter.arcaPrices(event.mids)
            if (snapshot || mids.isNotEmpty()) {
                retainedMids = if (snapshot) mids + (retainedMids ?: emptyMap()).filterKeys { it in marketDataRouter.status().directPriceMarkets }
                    else (retainedMids ?: emptyMap()) + mids
                marketDataRecorder.prices(mids, false, marketDataRouter.priceInterestMarkets)
                publishMarketDiagnostics()
                emit(event.copy(mids = mids))
            }
        } else if (marketDataRouter.arcaCandle(event)) {
            if (event.type == "candle.updated" || event.type == "candle.closed") marketDataRecorder.candle(false)
            publishMarketDiagnostics()
            emit(event)
        }
    }
    private val candleRefCoins = HashMap<String, MutableSet<String>>()
    private val oiRefCoins = HashMap<String, MutableSet<String>>()
    private val chartHistoryWatches = HashMap<String, ChartWatch>()

    // Watches created out-of-band (REST POST /aggregations/watch) that this
    // socket registered for delivery. Delivery is ownership-gated
    // server-side, so these are re-attached on every reconnect. The server
    // destroys a watch with the connection that registered it, so a
    // re-attach usually answers "unknown watch"; that answer is scoped to
    // the attach request and drops the id here.
    private val attachedWatches = LinkedHashSet<String>()
    private val attachRequests = HashMap<String, String>()

    // Aggregation watches created over the socket, by requestId. A request
    // parked while disconnected, or in flight on a socket that went away, is
    // (re)sent on the next live socket: the caller wants a watch on whichever
    // socket is current, and one created on a dead socket died with it.
    private class AggregationCreateRequest(
        val message: JsonObject,
        val deferred: CompletableDeferred<CreateWatchResponse>,
        var sentOn: WebSocket? = null,
    )
    private val aggregationCreateRequests = LinkedHashMap<String, AggregationCreateRequest>()
    private var nextRequestId = 0

    // Projection watch request/reply state. Replies are matched to callers by
    // requestId; a request issued while disconnected parks in
    // [pendingProjectionSends] until the next successful auth flushes it.
    private val pendingProjectionRequests = HashMap<String, CompletableDeferred<ProjectionWatchCreated>>()
    private val pendingProjectionSends = HashMap<String, JsonObject>()
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

    private val positionFillObservers = ConcurrentHashMap<UUID, (Fill, RealmEvent) -> Unit>()

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
        lock.withLock { publicSuspended = hiddenAtMs != null; syncPublicSourceLocked() }
        lock.withLock {
            shouldReconnect = true
            installLifecycleLocked()
            doConnectLocked()
        }
    }

    /** Connect only if not already connected, connecting, or waiting out a reconnect backoff. */
    public fun ensureConnected() {
        lock.withLock { ensureConnectedLocked() }
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
            publicSuspended = true; stopPublicSourceLocked()
            reconnectJob?.cancel(); reconnectJob = null
            stableJob?.cancel(); stableJob = null
            reconnectAttempt = 0
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
            retainedMids = null
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

    private val acknowledgedPaths = HashMap<String, WebSocket?>()
    private val acknowledgedOperations = HashMap<String, List<Operation>>()
    private val operationSnapshotHandlers = ConcurrentHashMap<UUID, (List<Operation>) -> Unit>()
    private val pathWatchRequests = HashMap<WebSocket?, MutableMap<String, String>>()
    private val pathReadyWaiters = HashMap<UUID, Pair<String, CompletableDeferred<Unit>>>()
    private val pathSnapshotWaiters = HashMap<UUID, Pair<String, CompletableDeferred<List<Operation>>>>()

    internal suspend fun awaitPathReady(path: String) {
        val id = UUID.randomUUID()
        val ready = CompletableDeferred<Unit>()
        lock.withLock {
            if (statusFlow.value == ConnectionStatus.CONNECTED && (pathRefs[path] ?: 0) > 0 && acknowledgedPaths.containsKey(path) && acknowledgedPaths[path] === webSocket) ready.complete(Unit)
            else pathReadyWaiters[id] = path to ready
        }
        try { ready.await() }
        finally { lock.withLock { pathReadyWaiters.remove(id) }; ready.cancel() }
    }

    private suspend fun awaitPathSnapshotOperations(path: String): List<Operation> {
        val id = UUID.randomUUID()
        val ready = CompletableDeferred<List<Operation>>()
        lock.withLock {
            val operations = acknowledgedOperations[path]
            if (statusFlow.value == ConnectionStatus.CONNECTED && (pathRefs[path] ?: 0) > 0 && acknowledgedPaths.containsKey(path) && acknowledgedPaths[path] === webSocket && operations != null) ready.complete(operations)
            else pathSnapshotWaiters[id] = path to ready
        }
        return try { ready.await() }
        finally { lock.withLock { pathSnapshotWaiters.remove(id) }; ready.cancel() }
    }

    internal suspend fun recoverPathReady(path: String) {
        watchPath(path)
        try {
            lock.withLock { sendPathWatchLocked(webSocket, path) }
            awaitPathReady(path)
        } finally { unwatchPath(path) }
    }

    internal suspend fun recoverPathSnapshotOperations(path: String): List<Operation> {
        watchPath(path)
        try {
            lock.withLock { sendPathWatchLocked(webSocket, path) }
            return awaitPathSnapshotOperations(path)
        } finally { unwatchPath(path) }
    }

    private fun sendPathWatchLocked(target: WebSocket?, path: String) {
        val requestId = "path-${UUID.randomUUID()}"
        pathWatchRequests.getOrPut(target) { HashMap() }[path] = requestId
        if (target === webSocket) { acknowledgedPaths.remove(path); acknowledgedOperations.remove(path) }
        sendMessage(target, buildJsonObject { put("action", "watch"); put("path", path); put("requestId", requestId) })
    }

    private fun acknowledgePathLocked(path: String) {
        if (statusFlow.value != ConnectionStatus.CONNECTED || (pathRefs[path] ?: 0) <= 0) return
        acknowledgedPaths[path] = webSocket
        val matching = pathReadyWaiters.filterValues { it.first == path }
        matching.forEach { (id, value) -> pathReadyWaiters.remove(id); value.second.complete(Unit) }
    }

    // Pong acknowledges transport readiness; only a correlated snapshot can
    // resolve operation-payload recovery.
    private fun acknowledgePathSnapshotLocked(path: String, operations: List<Operation>) {
        if (statusFlow.value != ConnectionStatus.CONNECTED || (pathRefs[path] ?: 0) <= 0) return
        acknowledgePathLocked(path)
        acknowledgedOperations[path] = operations
        operationSnapshotHandlers.values.forEach { it(operations) }
        val matching = pathSnapshotWaiters.filterValues { it.first == path }
        matching.forEach { (id, value) -> pathSnapshotWaiters.remove(id); value.second.complete(operations) }
    }

    // MARK: - Event type interest (ref-counted)

    // Realm event types subscribed by TYPE rather than by path. SDK-internal
    // owners (operation waits, order capture) hold these instead of a
    // realm-root watch, which would assemble a full-realm snapshot and put
    // every realm event on this socket. Counted so one owner's release cannot
    // unsubscribe a type another still needs.
    private val eventTypeRefs = HashMap<String, Int>()
    private val acknowledgedEventTypes = HashMap<String, WebSocket?>()
    // socket -> type -> latest request id. Only the latest request's
    // acknowledgement counts, so a stale one cannot pass for a fresh barrier.
    private val eventTypeRequests = HashMap<WebSocket?, MutableMap<String, String>>()
    private val eventTypeReadyWaiters = HashMap<UUID, Pair<List<String>, CompletableDeferred<Unit>>>()

    internal fun acquireEventTypes(types: List<String>) {
        lock.withLock {
            cancelIdleTimerLocked()
            val added = mutableListOf<String>()
            for (type in types) {
                val prev = eventTypeRefs[type] ?: 0
                eventTypeRefs[type] = prev + 1
                if (prev != 0) continue
                val pending = unsubJobs.remove("event:$type")
                if (pending != null) pending.cancel() else added += type
            }
            if (added.isNotEmpty()) {
                ensureConnectedLocked()
                sendEventTypesLocked(webSocket, added)
            }
        }
    }

    internal fun releaseEventTypes(types: List<String>) {
        lock.withLock {
            for (type in types) {
                val current = eventTypeRefs[type] ?: 0
                if (current > 1) { eventTypeRefs[type] = current - 1; continue }
                if (current != 1) continue
                eventTypeRefs.remove(type)
                val timerKey = "event:$type"
                unsubJobs[timerKey] = scope.launch {
                    delay(UNSUB_DEBOUNCE_MS)
                    if (!isActive) return@launch
                    finishEventTypeRelease(type, timerKey)
                }
            }
        }
    }

    private fun finishEventTypeRelease(type: String, timerKey: String) {
        lock.withLock {
            unsubJobs.remove(timerKey)
            if (!eventTypeRefs.containsKey(type)) {
                acknowledgedEventTypes.remove(type)
                eventTypeRequests[webSocket]?.remove(type)
                sendMessage(buildJsonObject {
                    put("action", "unsubscribe_events")
                    put("types", buildJsonArray { add(type) })
                })
            }
            maybeStartIdleTimerLocked()
        }
    }

    /**
     * The server's acknowledgement of a type subscription on the current
     * connection is the registration barrier: anything published after it
     * reaches this socket. Sending the subscription alone does not close the gap.
     */
    internal suspend fun awaitEventTypesReady(types: List<String>) {
        val id = UUID.randomUUID()
        val ready = CompletableDeferred<Unit>()
        lock.withLock {
            if (eventTypesReadyLocked(types)) ready.complete(Unit) else eventTypeReadyWaiters[id] = types to ready
        }
        try { ready.await() }
        finally { lock.withLock { eventTypeReadyWaiters.remove(id) }; ready.cancel() }
    }

    /**
     * Actual-gap recovery requires a newly acknowledged subscription, even if
     * an earlier acknowledgement is cached — the type-routed counterpart of
     * [recoverPathReady].
     */
    internal suspend fun recoverEventTypesReady(types: List<String>) {
        acquireEventTypes(types)
        try {
            lock.withLock { sendEventTypesLocked(webSocket, types) }
            awaitEventTypesReady(types)
        } finally { releaseEventTypes(types) }
    }

    private fun eventTypesReadyLocked(types: List<String>): Boolean =
        statusFlow.value == ConnectionStatus.CONNECTED && types.all {
            (eventTypeRefs[it] ?: 0) > 0 && acknowledgedEventTypes.containsKey(it) && acknowledgedEventTypes[it] === webSocket
        }

    private fun sendEventTypesLocked(target: WebSocket?, types: Collection<String>) {
        val requestId = "events-${UUID.randomUUID()}"
        val requests = eventTypeRequests.getOrPut(target) { HashMap() }
        types.forEach { requests[it] = requestId }
        if (target === webSocket) types.forEach { acknowledgedEventTypes.remove(it) }
        sendMessage(target, buildJsonObject {
            put("action", "subscribe_events")
            put("types", buildJsonArray { types.forEach { add(it) } })
            put("requestId", requestId)
        })
    }

    private fun acknowledgeEventTypesLocked(types: Collection<String>) {
        if (statusFlow.value != ConnectionStatus.CONNECTED) return
        types.filter { (eventTypeRefs[it] ?: 0) > 0 }.forEach { acknowledgedEventTypes[it] = webSocket }
        val ready = eventTypeReadyWaiters.filterValues { eventTypesReadyLocked(it.first) }
        ready.forEach { (id, value) -> eventTypeReadyWaiters.remove(id); value.second.complete(Unit) }
    }

    private fun handleEventsSubscribed(obj: JsonObject) {
        val requestId = obj["requestId"]?.jsonPrimitive?.contentOrNull ?: return
        lock.withLock {
            val types = eventTypeRequests[webSocket]?.filterValues { it == requestId }?.keys?.toList().orEmpty()
            if (types.isNotEmpty()) acknowledgeEventTypesLocked(types)
        }
    }

    internal fun onOperationSnapshot(handler: (List<Operation>) -> Unit): UUID {
        val id = UUID.randomUUID(); operationSnapshotHandlers[id] = handler; return id
    }

    internal fun removeOperationSnapshotHandler(id: UUID) { operationSnapshotHandlers.remove(id) }

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
                    sendPathWatchLocked(webSocket, path)
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
                acknowledgedPaths.remove(path); acknowledgedOperations.remove(path)
                pathWatchRequests[webSocket]?.remove(path)
                sendMessage(buildJsonObject { put("action", "unwatch"); put("path", path) })
            }
            maybeStartIdleTimerLocked()
        }
    }

    /**
     * Acquire interest in mid price updates.
     *
     * The Arca socket subscribes only to the markets its consumers have
     * declared through [registerPriceMarkets] / [updatePriceMarkets] — the
     * same interest set the direct Hyperliquid provider already subscribes
     * per coin — so a phone pays for the markets on its screen, not for
     * every market on the venue. `allMarkets = true` (the pre-existing
     * meaning of a bare `acquireMids`) is for a consumer that cannot name
     * its markets and genuinely wants every mid; while any such consumer is
     * live the socket subscribes to everything. Consumers that know their
     * markets (a price watch with a market list, an account watch with
     * positions, a projection watch with rows) pass `false` and keep their
     * interest owner current.
     */
    public fun acquireMids(exchange: String, allMarkets: Boolean = true) {
        lock.withLock {
            publicSuspended = hiddenAtMs != null
            cancelIdleTimerLocked()
            midsExchange = exchange
            midsRefs += 1
            if (allMarkets) midsAllRefs += 1
            unsubJobs.remove("mids")?.cancel()
            ensureConnectedLocked()
            syncPublicSourceLocked()
        }
    }

    /** Release interest in mid price updates. `allMarkets` must match the value passed to [acquireMids]. */
    public fun releaseMids(allMarkets: Boolean = true) {
        lock.withLock {
            midsRefs = maxOf(0, midsRefs - 1)
            if (allMarkets) midsAllRefs = maxOf(0, midsAllRefs - 1)
            scheduleMidsNarrowingLocked()
        }
    }

    /**
     * Narrowing (or dropping) the Arca mids subscription is debounced so a
     * screen that releases and re-acquires within a frame does not churn a
     * subscribe/unsubscribe pair and its snapshot. Widening is never
     * debounced: a consumer that just declared a market needs its price.
     */
    private fun scheduleMidsNarrowingLocked() {
        unsubJobs.remove("mids")?.cancel()
        unsubJobs["mids"] = scope.launch {
            delay(UNSUB_DEBOUNCE_MS)
            if (!isActive) return@launch
            finishMidsRelease()
        }
    }

    private fun finishMidsRelease() {
        lock.withLock {
            unsubJobs.remove("mids")
            if (midsRefs == 0) {
                if (subscribedMids != null) {
                    subscribedMids = null
                    retainedMids = null
                    sendMessage(buildJsonObject { put("action", "unsubscribe_mids") })
                }
            } else {
                syncArcaMidsLocked()
            }
            maybeStartIdleTimerLocked()
        }
    }

    /**
     * The coin set the Arca socket should hold right now: null when no
     * consumer needs mids (or none has declared a market yet), an empty
     * list for every market while an all-markets consumer is live,
     * otherwise the sorted union of declared interests.
     */
    private fun desiredArcaMidsLocked(): List<String>? {
        if (midsRefs <= 0) return null
        if (midsAllRefs > 0) return emptyList()
        val markets = marketDataRouter.priceInterestMarkets
        if (markets.isEmpty()) return null
        return markets.sorted()
    }

    /**
     * Reconciles the Arca mids subscription with the desired coin set,
     * sending `subscribe_mids` only when the set actually changed. The
     * server treats a repeated `subscribe_mids` as a replacement and
     * answers it with a snapshot of the new set, which seeds any market
     * that was just added. A subscription made directly through
     * [subscribeMids] (no consumer refs) is left alone: [unsubscribeMids]
     * is its counterpart.
     */
    private fun syncArcaMidsLocked() {
        if (midsRefs <= 0) return
        val desired = desiredArcaMidsLocked()
        val current = subscribedMids
        when {
            desired == null && current == null -> return
            desired == null -> {
                subscribedMids = null
                retainedMids = null
                sendMessage(buildJsonObject { put("action", "unsubscribe_mids") })
            }
            current != null && current.first == midsExchange && current.second == desired -> return
            else -> {
                subscribedMids = midsExchange to desired
                sendMessage(authlessSubscribeMids(midsExchange, desired))
            }
        }
    }

    public fun acquireCandles(coins: List<String>, intervals: List<CandleInterval>) {
        lock.withLock {
            marketDataRouter.acquireCandles(coins, intervals); syncPublicSourceLocked()
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
            marketDataRouter.releaseCandles(coins, intervals); syncPublicSourceLocked()
            for (coin in coins) {
                val ivs = candleRefCoins[coin] ?: continue
                intervals.filterNot { marketDataRouter.candleRetained(coin, it) }.forEach { ivs.remove(it.wire) }
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
     * Create a server-side projection watch for the accounts in [scope] and
     * suspend until the server replies with the snapshot of that scope. The
     * scope only narrows the projection's registration; an empty scope is
     * an empty watch. The watch is connection-scoped: it dies with the
     * socket and callers must re-create it on reconnect or rotation (see
     * [rotatedStream]).
     */
    public suspend fun createProjectionWatch(projection: String, scope: ProjectionWatchScope = ProjectionWatchScope()): ProjectionWatchCreated =
        projectionRequest("watch_projection '$projection'") { id -> watchProjectionMsg(projection, scope, id) }

    /**
     * Replace a live projection watch's interest set in place and suspend
     * until the server replies with the snapshot of the NEW scope. The
     * watch keeps its ID; the caller swaps the rows wholesale.
     */
    public suspend fun updateProjectionWatch(watchId: String, scope: ProjectionWatchScope): ProjectionWatchCreated =
        projectionRequest("update_projection_watch '$watchId'") { id -> updateProjectionWatchMsg(watchId, scope, id) }

    private suspend fun projectionRequest(label: String, build: (String) -> JsonObject): ProjectionWatchCreated {
        val deferred = CompletableDeferred<ProjectionWatchCreated>()
        val requestId = lock.withLock {
            cancelIdleTimerLocked()
            ensureConnectedLocked()
            nextProjectionRequestId += 1
            val id = "proj-req-$nextProjectionRequestId"
            val message = build(id)
            pendingProjectionRequests[id] = deferred
            if (statusFlow.value == ConnectionStatus.CONNECTED) {
                sendMessage(message)
            } else {
                pendingProjectionSends[id] = message
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
                "$label: timed out waiting for the server reply",
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
            // Before auth the server answers "not authenticated" with no
            // requestId, which reads as a connection-level failure. Auth
            // attaches every registered watch, so nothing is lost by waiting.
            if (statusFlow.value == ConnectionStatus.CONNECTED) sendAttachLocked(webSocket, watchId)
        }
    }

    /**
     * Create a standalone aggregation watch on this socket and suspend until
     * the server replies with the initial aggregation.
     *
     * The watch registry is per pod and a socket only receives updates for
     * watches on its own pod, so a watch created here is always deliverable,
     * where a REST-created one is silent whenever the REST call and the socket
     * land on different pods. The watch is connection-scoped: the server
     * destroys it when the socket closes, so callers re-create it on reconnect
     * or rotation (see [rotatedStream]).
     */
    internal suspend fun createAggregationWatch(
        sources: List<AggregationSource>,
        flowsSince: String? = null,
    ): CreateWatchResponse {
        val deferred = CompletableDeferred<CreateWatchResponse>()
        val requestId = lock.withLock {
            cancelIdleTimerLocked()
            ensureConnectedLocked()
            nextRequestId += 1
            val id = "agg-req-$nextRequestId"
            val request = AggregationCreateRequest(createAggregationWatchMsg(sources, flowsSince, id), deferred)
            aggregationCreateRequests[id] = request
            if (statusFlow.value == ConnectionStatus.CONNECTED) sendAggregationCreateLocked(webSocket, request)
            id
        }
        try {
            return withTimeout(AGGREGATION_REQUEST_TIMEOUT_MS) { deferred.await() }
        } catch (e: TimeoutCancellationException) {
            throw ArcaException.Unknown(
                "WS_REQUEST_TIMEOUT",
                "create_aggregation_watch: timed out waiting for the server reply",
            )
        } finally {
            lock.withLock { aggregationCreateRequests.remove(requestId) }
        }
    }

    /** Tear down a watch made by [createAggregationWatch]. Best-effort; a no-op while disconnected. */
    internal fun destroyAggregationWatch(watchId: String) {
        if (watchId.isEmpty()) return
        lock.withLock {
            // A disconnected socket already took the watch with it.
            if (statusFlow.value == ConnectionStatus.CONNECTED) {
                sendMessage(buildJsonObject { put("action", "destroy_aggregation_watch"); put("watchId", watchId) })
            }
            maybeStartIdleTimerLocked()
        }
    }

    private fun sendAttachLocked(target: WebSocket?, watchId: String) {
        nextRequestId += 1
        val id = "attach-req-$nextRequestId"
        attachRequests[id] = watchId
        sendMessage(target, attachAggregationWatchMsg(watchId, id))
    }

    private fun sendAggregationCreateLocked(target: WebSocket?, request: AggregationCreateRequest) {
        if (target == null) return
        request.sentOn = target
        sendMessage(target, request.message)
    }

    /** Send every create request not yet sent on [target], the newly live socket. */
    private fun flushAggregationCreatesLocked(target: WebSocket?) {
        aggregationCreateRequests.values.forEach { if (it.sentOn !== target) sendAggregationCreateLocked(target, it) }
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
            chartHistoryWatches.isNotEmpty() || attachedWatches.isNotEmpty() || eventTypeRefs.isNotEmpty()

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

    public fun orderExecutionEvents(): Flow<RealmEvent> = filtered { event ->
        event.takeIf { it.type in setOf("order.updated", "operation.updated", "fill.previewed", "fill.recorded") }
    }

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

    /**
     * A stream of mid price updates.
     *
     * The current merged price map is replayed as the first element when one
     * is held, so a collector that subscribes after the server's
     * `mids.snapshot` starts from the same prices as the one that received it
     * rather than from nothing. Each collector has only the newest pending
     * complete snapshot. Sparse deltas are merged before conflation, so a slow
     * valuation consumer cannot lose a quiet market's last price.
     */
    public fun midsEvents(): Flow<Map<String, String>> = midsSnapshots.mapNotNull { it.prices }

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
        val fill = event.executionFill
        if (event.type in setOf(EventType.FILL_PREVIEWED.wire, EventType.FILL_RECORDED.wire) && fill != null) fill to event else null
    }

    /** Inline display-evidence tap, ahead of the lossy general-purpose event bus. */
    internal fun observePositionFills(handler: (Fill, RealmEvent) -> Unit): UUID {
        val id = UUID.randomUUID(); positionFillObservers[id] = handler; return id
    }

    internal fun removePositionFillObserver(id: UUID) { positionFillObservers.remove(id) }

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
        lock.withLock { hiddenAtMs = System.currentTimeMillis(); publicSuspended = true; stopPublicSourceLocked() }
    }

    /** Host hook: notify the manager the app returned to the foreground. */
    public fun handleAppWillEnterForeground() {
        val hiddenDuration = lock.withLock {
            val hidden = hiddenAtMs ?: return
            hiddenAtMs = null
            publicSuspended = false; syncPublicSourceLocked()
            // A user returning to the app should not wait out a backoff that
            // grew while it was hidden. The attempt count is kept, so a
            // connect that fails again still backs off.
            if (shouldReconnect && webSocket == null && reconnectJob != null) doConnectLocked()
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
        // A scheduled reconnect is already connecting; jumping it would undo the backoff.
        if (webSocket != null || reconnectJob != null) return
        shouldReconnect = true
        installLifecycleLocked()
        doConnectLocked()
    }

    private fun doConnectLocked() {
        // A scheduled reconnect firing after this would replace the socket
        // opened here, possibly mid-auth.
        reconnectJob?.cancel()
        reconnectJob = null
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
        lock.withLock { marketDataRecorder.bytes(text.toByteArray(Charsets.UTF_8).size, false); publishMarketDiagnostics() }
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
                "events_subscribed" -> { handleEventsSubscribed(obj); return }
                "projection_watch_created", "projection_watch_updated" -> { handleProjectionWatchCreated(obj); return }
                "aggregation_watch_created" -> { handleAggregationWatchCreated(obj); return }
                "aggregation_watch_attached" -> {
                    obj["requestId"]?.jsonPrimitive?.contentOrNull?.let { id -> lock.withLock { attachRequests.remove(id) } }
                    return
                }
                "error" -> { handleServerError(obj); return }
                "mids.snapshot" -> {
                    val midsRaw = obj["mids"]?.jsonObject ?: return
                    val mids = midsRaw.mapValues { it.value.jsonPrimitive.content }
                    // A snapshot is the authoritative current state for
                    // whatever the socket is subscribed to, so it replaces the
                    // retained map rather than merging into it.
                    emitArcaMarketEvent(RealmEvent(type = EventType.MIDS_UPDATED.wire, mids = mids), snapshot = true)
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
                        emitArcaMarketEvent(
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

        runCatching { arcaJson.decodeFromString(RealmEvent.serializer(), text) }.getOrNull()?.let { event ->
            emitArcaMarketEvent(event)
        }
    }

    private fun handleAuthenticated(obj: JsonObject?) {
        lock.withLock {
            acknowledgedPaths.clear(); acknowledgedOperations.clear()
            pathWatchRequests.clear()
            acknowledgedEventTypes.clear(); eventTypeRequests.clear()
            log.info("websocket") { "authenticated" }
            scheduleBackoffResetLocked()
            lastDeliverySeq = 0
            attachRequests.clear()
            if (obj != null) readServerLifetimeLocked(obj)
            setStatusLocked(ConnectionStatus.CONNECTED)
            startHeartbeatLocked()
            resubscribeAllLocked(webSocket)
            flushAggregationCreatesLocked(webSocket)
            scheduleRotationLocked()
        }
        // Notify subscribers AFTER all subscriptions are re-issued.
        authenticatedHandlers.values.forEach { it() }
        authenticatedFlow.tryEmit(Unit)
    }

    private fun resubscribeAllLocked(target: WebSocket?) {
        // The mids subscription is re-derived from the live consumers'
        // interests rather than replayed from the last set sent, so a
        // socket that comes up mid-change subscribes to what is wanted now.
        val desiredMids = desiredArcaMidsLocked()
        if (desiredMids != null) {
            subscribedMids = midsExchange to desiredMids
            sendMessage(target, authlessSubscribeMids(midsExchange, desiredMids))
        } else {
            subscribedMids?.let { sendMessage(target, authlessSubscribeMids(it.first, it.second)) }
        }
        subscribedCandles?.let { sendMessage(target, subscribeCandlesMsg(it.first, it.second.map { iv -> iv.wire })) }
        subscribedOI?.let { sendMessage(target, subscribeOIMsg(it.first, it.second.map { iv -> iv.wire })) }
        if (candleRefCoins.isNotEmpty() && subscribedCandles == null) {
            syncCandleSubscriptionLocked(target)
        }
        if (oiRefCoins.isNotEmpty() && subscribedOI == null) {
            syncOISubscriptionLocked(target)
        }
        pathRefs.keys.forEach { path ->
            sendPathWatchLocked(target, path)
        }
        if (eventTypeRefs.isNotEmpty()) sendEventTypesLocked(target, eventTypeRefs.keys.toList())
        chartHistoryWatches.forEach { (watchId, req) ->
            sendMessage(target, watchChartHistoryMsg(watchId, req.target, req.kind, req.objectId))
        }
        // Re-register REST-created watches. Each attach carries a requestId,
        // so an "unknown watch" answer drops that watch instead of the socket.
        attachedWatches.forEach { watchId -> sendAttachLocked(target, watchId) }
        // Projection watch requests parked while disconnected go out now that
        // auth completed; the replies resolve the callers' pending requests.
        if (pendingProjectionSends.isNotEmpty()) {
            pendingProjectionSends.values.forEach { message -> sendMessage(target, message) }
            pendingProjectionSends.clear()
        }
    }

    private fun handleServerError(obj: JsonObject) {
        val message = obj["message"]?.jsonPrimitive?.contentOrNull ?: "Unknown WebSocket error"

        // An error carrying a requestId is scoped to that request (a
        // watch_projection the caller isn't authorized for, an attach for a
        // watch that no longer exists) and leaves the connection serving,
        // even when the request is one this client no longer tracks.
        // Dropping the socket for it reconnected straight into the same
        // error, every second, for as long as the screen stayed open.
        val requestId = obj["requestId"]?.jsonPrimitive?.contentOrNull
        if (requestId != null) {
            handleRequestError(requestId, message)
            return
        }

        // After authentication every error answers one request (a
        // non-canonical market id, a path outside the credential's scope).
        // The connection is fine, and a reconnect would replay that request
        // straight into the same error.
        if (statusFlow.value == ConnectionStatus.CONNECTED) {
            log.warning("websocket", metadata = mapOf("message" to message)) { "request rejected" }
            return
        }

        log.error("websocket", metadata = mapOf("message" to message)) { "server error" }
        lock.withLock {
            webSocket?.cancel()
            webSocket = null
            dropLiveSocketLocked()
        }
    }

    private fun handleRequestError(requestId: String, message: String) {
        val meta = mapOf("message" to message, "requestId" to requestId)
        val projection = lock.withLock {
            pendingProjectionSends.remove(requestId)
            pendingProjectionRequests.remove(requestId)
        }
        if (projection != null) {
            log.warning("websocket", metadata = meta) { "projection watch request rejected" }
            projection.completeExceptionally(ArcaException.Unknown("WS_REQUEST_ERROR", message))
            return
        }
        val create = lock.withLock { aggregationCreateRequests.remove(requestId) }
        if (create != null) {
            log.warning("websocket", metadata = meta) { "aggregation watch request rejected" }
            create.deferred.completeExceptionally(ArcaException.Unknown("WS_REQUEST_ERROR", message))
            return
        }
        val attached = lock.withLock {
            attachRequests.remove(requestId)?.also { attachedWatches.remove(it); maybeStartIdleTimerLocked() }
        }
        if (attached != null) {
            log.warning("websocket", metadata = meta + ("watchId" to attached)) {
                "aggregation watch attach rejected; the watch has to be re-created"
            }
            return
        }
        log.warning("websocket", metadata = meta) { "request rejected" }
    }

    private fun handleAggregationWatchCreated(obj: JsonObject) {
        val requestId = obj["requestId"]?.jsonPrimitive?.contentOrNull ?: return
        val request = lock.withLock { aggregationCreateRequests.remove(requestId) } ?: return
        val watchId = obj["watchId"]?.jsonPrimitive?.contentOrNull
        val aggregation = obj["aggregation"]?.let { el ->
            runCatching { arcaJson.decodeFromJsonElement(PathAggregation.serializer(), el) }.getOrNull()
        }
        if (watchId.isNullOrEmpty() || aggregation == null) {
            request.deferred.completeExceptionally(
                ArcaException.Unknown("WS_REQUEST_ERROR", "create_aggregation_watch: reply carried no watch or aggregation"),
            )
            return
        }
        request.deferred.complete(CreateWatchResponse(watchId = WatchId(watchId), aggregation = aggregation))
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
        obj["path"]?.jsonPrimitive?.contentOrNull?.let { path -> lock.withLock {
            val requestId = obj["requestId"]?.jsonPrimitive?.contentOrNull
            if (requestId != null && pathWatchRequests[webSocket]?.get(path) == requestId) {
                val rows = (obj["operations"] as? JsonArray).orEmpty() + (obj["bufferedOperations"] as? JsonArray).orEmpty()
                val operations = rows.mapNotNull { runCatching { arcaJson.decodeFromJsonElement(Operation.serializer(), it) }.getOrNull() }
                acknowledgePathSnapshotLocked(path, operations)
            }
        } }
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
        if (event.type == EventType.FILL_RECORDED.wire) event.recordedFill?.let { fill ->
            positionFillObservers.values.toList().forEach { observer -> observer(fill, event) }
        }
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
        lateinit var job: Job
        job = scope.launch {
            delay((delaySeconds * 1000).toLong())
            if (!isActive) return@launch
            lock.withLock {
                // A direct connect in the meantime superseded this one.
                if (reconnectJob !== job) return@withLock
                reconnectJob = null
                doConnectLocked()
            }
        }
        reconnectJob = job
    }

    /** Reset the backoff once the live socket has stayed up for [STABLE_CONNECTION_MS]. */
    private fun scheduleBackoffResetLocked() {
        stableJob?.cancel()
        val live = webSocket
        stableJob = scope.launch {
            delay(STABLE_CONNECTION_MS)
            if (!isActive) return@launch
            lock.withLock {
                if (live != null && webSocket === live) reconnectAttempt = 0
                stableJob = null
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
        handoffAuthenticated = false
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
                // A rejected resubscribe request (an attach for a watch that
                // lives on the retiring connection's pod) doesn't make the
                // replacement unusable.
                val requestId = obj["requestId"]?.jsonPrimitive?.contentOrNull
                if (requestId != null) {
                    handleRequestError(requestId, message)
                    return
                }
                // Once authenticated, an error answers one resubscribe request.
                if (lock.withLock { handoffWebSocket === socket && handoffAuthenticated }) {
                    log.warning("websocket", metadata = mapOf("message" to message)) { "request rejected" }
                    return
                }
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
            handoffAuthenticated = true
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
            acknowledgedOperations.clear()
            pathRefs.keys.forEach { path ->
                if (pathWatchRequests[socket]?.containsKey(path) == true) acknowledgePathLocked(path)
                else sendPathWatchLocked(socket, path)
            }
            // Restart payload recovery on the new connection; pong carries no snapshot.
            pathSnapshotWaiters.values.map { it.first }.toSet().forEach { sendPathWatchLocked(socket, it) }
            pathWatchRequests.keys.retainAll(setOf(socket))
            // Same barrier for type subscriptions: the warming batch's are
            // proven registered by the pong; any taken after it need their own.
            val (warmedTypes, unwarmedTypes) = eventTypeRefs.keys.partition { eventTypeRequests[socket]?.containsKey(it) == true }
            if (warmedTypes.isNotEmpty()) acknowledgeEventTypesLocked(warmedTypes)
            if (unwarmedTypes.isNotEmpty()) sendEventTypesLocked(socket, unwarmedTypes)
            eventTypeRequests.keys.retainAll(setOf(socket))
            // A create still in flight on the retiring socket would answer
            // there, where nothing listens any more.
            flushAggregationCreatesLocked(socket)
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
        marketDataRecorder.connected(newStatus == ConnectionStatus.CONNECTED)
        publishMarketDiagnostics(true)
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

    private fun watchProjectionMsg(projection: String, scope: ProjectionWatchScope, requestId: String): JsonObject = buildJsonObject {
        put("action", "watch_projection")
        put("projection", projection)
        putProjectionScope(scope)
        put("requestId", requestId)
    }

    private fun updateProjectionWatchMsg(watchId: String, scope: ProjectionWatchScope, requestId: String): JsonObject = buildJsonObject {
        put("action", "update_projection_watch")
        put("watchId", watchId)
        putProjectionScope(scope)
        put("requestId", requestId)
    }

    /** Scope fields only when present: an empty scope goes over the wire as exactly that. */
    private fun JsonObjectBuilder.putProjectionScope(scope: ProjectionWatchScope) {
        if (scope.paths.isNotEmpty()) put("paths", buildJsonArray { scope.paths.forEach { add(it) } })
        if (!scope.prefix.isNullOrEmpty()) put("prefix", scope.prefix)
    }

    private fun attachAggregationWatchMsg(watchId: String, requestId: String): JsonObject = buildJsonObject {
        put("action", "attach_aggregation_watch")
        put("watchId", watchId)
        put("requestId", requestId)
    }

    // The socket handler reads a `paths` source's paths from an array, where
    // REST takes the single path in `value`.
    private fun createAggregationWatchMsg(sources: List<AggregationSource>, flowsSince: String?, requestId: String): JsonObject =
        buildJsonObject {
            put("action", "create_aggregation_watch")
            put("sources", buildJsonArray {
                sources.forEach { source ->
                    add(buildJsonObject {
                        put("type", source.type.wire)
                        if (source.type == AggregationSourceType.PATHS) {
                            put("paths", buildJsonArray { add(source.value) })
                        } else {
                            put("value", source.value)
                        }
                    })
                }
            })
            if (flowsSince != null) put("flowsSince", flowsSince)
            put("requestId", requestId)
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

        // Budget for a create_aggregation_watch reply, including any wait for
        // the socket to authenticate.
        const val AGGREGATION_REQUEST_TIMEOUT_MS = 10_000L

        // A connection must stay up this long before the reconnect backoff
        // resets. Resetting at auth let a socket that authenticated and then
        // failed reconnect every second indefinitely.
        const val STABLE_CONNECTION_MS = 30_000L
    }
}

/**
 * The interest set of a projection watch: which of the accounts a projection
 * covers this subscriber wants deltas for. It only ever narrows the
 * projection's registered resources — a path outside them is dropped, a
 * prefix is intersected with them, and the two compose as AND. An empty
 * scope (neither field) is a watch that delivers nothing until
 * [ProjectionWatchStream.setPaths] names one; it is never "everyone".
 */
public data class ProjectionWatchScope(
    /** Exact object paths to watch (max 500) — a leaderboard roster, a set of followed traders. */
    public val paths: List<String> = emptyList(),
    /**
     * A sub-tree of the projection to watch (`"/users/alice"`). Pass the
     * projection's own prefix to opt into the whole covered set explicitly;
     * on a realm-wide projection that is the shape to avoid on a device.
     */
    public val prefix: String? = null,
) {
    public val isEmpty: Boolean get() = paths.isEmpty() && prefix.isNullOrEmpty()
}

/** The server's reply to a `watch_projection` or `update_projection_watch` request. */
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
