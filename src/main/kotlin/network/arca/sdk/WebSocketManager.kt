package network.arca.sdk

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
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
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.Candle
import network.arca.sdk.models.CandleEvent
import network.arca.sdk.models.CandleInterval
import network.arca.sdk.models.ConnectionStatus
import network.arca.sdk.models.EventType
import network.arca.sdk.models.ExchangeState
import network.arca.sdk.models.Fill
import network.arca.sdk.models.FundingPayment
import network.arca.sdk.models.ObjectValuation
import network.arca.sdk.models.Operation
import network.arca.sdk.models.PathAggregation
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
    private val httpClient: OkHttpClient,
    private val getToken: (suspend () -> String)? = null,
    private val maxReconnectDelaySeconds: Double = 30.0,
    private val log: ArcaLogger = ArcaLogger.disabled,
    private val lifecycleBridge: AppLifecycleBridge? = null,
) {
    private val wsUrl = baseUrl.trimEnd('/').toHttpUrl().newBuilder()
        .addPathSegments("api/v1/ws").build()

    private val lock = ReentrantLock()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    @Volatile private var token: String = token
    @Volatile private var webSocket: WebSocket? = null

    private var subscribedMids: Pair<String, List<String>>? = null
    private var subscribedCandles: Pair<List<String>, List<CandleInterval>>? = null
    private var shouldReconnect = false
    private var reconnectAttempt = 0
    private var reconnectJob: Job? = null

    private val pathRefs = HashMap<String, Int>()
    private var midsRefs = 0
    private var midsExchange = "sim"
    private val candleRefCoins = HashMap<String, MutableSet<String>>()
    private val chartHistoryWatches = HashMap<String, ChartWatch>()
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

    private val bus = MutableSharedFlow<RealmEvent>(
        replay = 0,
        extraBufferCapacity = 1024,
        onBufferOverflow = kotlinx.coroutines.channels.BufferOverflow.DROP_OLDEST,
    )
    private val statusFlow = MutableStateFlow(ConnectionStatus.DISCONNECTED)
    private val resumeFlow = MutableSharedFlow<Double>(extraBufferCapacity = 16)
    private val authenticatedFlow = MutableSharedFlow<Unit>(extraBufferCapacity = 16)

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

    private fun syncCandleSubscriptionLocked() {
        if (candleRefCoins.isEmpty()) {
            subscribedCandles = null
            sendMessage(buildJsonObject { put("action", "unsubscribe_candles") })
            return
        }
        val allCoins = candleRefCoins.keys.toList()
        val allIntervals = mutableSetOf<String>()
        candleRefCoins.values.forEach { allIntervals.addAll(it) }
        val intervals = allIntervals.mapNotNull { CandleInterval.fromWire(it) }
        subscribedCandles = allCoins to intervals
        sendMessage(subscribeCandlesMsg(allCoins, intervals.map { it.wire }))
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

    private fun hasAnyInterestLocked(): Boolean =
        pathRefs.isNotEmpty() || midsRefs > 0 || candleRefCoins.isNotEmpty() || chartHistoryWatches.isNotEmpty()

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
            setStatusLocked(ConnectionStatus.DISCONNECTED)
            if (shouldReconnect) scheduleReconnectLocked()
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
        val existing = webSocket
        webSocket = null
        existing?.cancel()

        setStatusLocked(ConnectionStatus.CONNECTING)
        log.debug("websocket", metadata = mapOf("url" to wsUrl.toString(), "realmId" to realmId)) { "connecting" }

        val request = Request.Builder().url(wsUrl).build()
        val ws = httpClient.newWebSocket(request, SocketListener())
        webSocket = ws

        val gt = getToken
        if (gt != null) {
            scope.launch {
                try {
                    val fresh = gt()
                    lock.withLock { token = fresh }
                    sendMessage(authMsg(fresh))
                } catch (e: Throwable) {
                    log.error("websocket", e) { "token refresh failed on reconnect, falling back to cached token" }
                    sendMessage(authMsg(token))
                }
            }
        } else {
            sendMessage(authMsg(token))
        }
    }

    private fun handleMessage(text: String) {
        lastMessageAtMs = System.currentTimeMillis()
        val obj = runCatching { arcaJson.parseToJsonElement(text).jsonObject }.getOrNull()

        if (obj != null) {
            when (obj["type"]?.jsonPrimitive?.contentOrNull ?: "") {
                "pong" -> return
                "authenticated" -> { handleAuthenticated(); return }
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

    private fun handleAuthenticated() {
        lock.withLock {
            log.info("websocket") { "authenticated" }
            reconnectAttempt = 0
            lastDeliverySeq = 0
            setStatusLocked(ConnectionStatus.CONNECTED)
            startHeartbeatLocked()

            subscribedMids?.let { sendMessage(authlessSubscribeMids(it.first, it.second)) }
            subscribedCandles?.let { sendMessage(subscribeCandlesMsg(it.first, it.second.map { iv -> iv.wire })) }
            if (midsRefs > 0 && subscribedMids == null) {
                sendMessage(authlessSubscribeMids(midsExchange, emptyList()))
            }
            if (candleRefCoins.isNotEmpty() && subscribedCandles == null) {
                syncCandleSubscriptionLocked()
            }
            pathRefs.keys.forEach { path ->
                sendMessage(buildJsonObject { put("action", "watch"); put("path", path) })
            }
            chartHistoryWatches.forEach { (watchId, req) ->
                sendMessage(watchChartHistoryMsg(watchId, req.target, req.kind, req.objectId))
            }
        }
        // Notify subscribers AFTER all subscriptions are re-issued.
        authenticatedHandlers.values.forEach { it() }
        authenticatedFlow.tryEmit(Unit)
    }

    private fun handleServerError(obj: JsonObject) {
        val message = obj["message"]?.jsonPrimitive?.contentOrNull ?: "Unknown WebSocket error"
        log.error("websocket", metadata = mapOf("message" to message)) { "server error" }
        lock.withLock {
            setStatusLocked(ConnectionStatus.DISCONNECTED)
            webSocket?.cancel()
            webSocket = null
            if (shouldReconnect) scheduleReconnectLocked()
        }
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
                setStatusLocked(ConnectionStatus.DISCONNECTED)
                if (shouldReconnect) scheduleReconnectLocked()
                return
            }
            sendMessage(buildJsonObject { put("action", "ping") })
        }
    }

    // MARK: - Messaging + status

    private fun sendMessage(message: JsonObject) {
        val ws = webSocket ?: return
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

    private fun watchChartHistoryMsg(watchId: String, target: String, kind: String, objectId: String?): JsonObject =
        buildJsonObject {
            put("action", "watch_chart_history")
            put("watchId", watchId)
            put("target", target)
            put("kind", kind)
            if (objectId != null) put("objectId", objectId)
        }

    // MARK: - OkHttp listener

    private inner class SocketListener : WebSocketListener() {
        override fun onMessage(webSocket: WebSocket, text: String) {
            if (this@WebSocketManager.webSocket === webSocket) handleMessage(text)
        }

        override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
            if (this@WebSocketManager.webSocket === webSocket) handleMessage(bytes.utf8())
        }

        override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
            lock.withLock {
                if (this@WebSocketManager.webSocket !== webSocket) return
                log.warning("websocket", t) { "receive loop error" }
                this@WebSocketManager.webSocket = null
                setStatusLocked(ConnectionStatus.DISCONNECTED)
                if (shouldReconnect) scheduleReconnectLocked()
            }
        }

        override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
            lock.withLock {
                if (this@WebSocketManager.webSocket !== webSocket) return
                this@WebSocketManager.webSocket = null
                setStatusLocked(ConnectionStatus.DISCONNECTED)
                if (shouldReconnect) scheduleReconnectLocked()
            }
        }
    }

    private companion object {
        const val UNSUB_DEBOUNCE_MS = 100L
        const val IDLE_DISCONNECT_MS = 60_000L
        const val PING_INTERVAL_MS = 30_000L
        const val STALE_THRESHOLD_S = 45.0
        const val RESUME_HIDDEN_THRESHOLD_S = 5.0
        const val RESUME_PING_TIMEOUT_MS = 2_000L
    }
}

/** Object-valuation event payload: a valuation plus its path, watch id, and raw event. */
public data class ObjectValuationEvent(
    public val valuation: ObjectValuation,
    public val path: String,
    public val watchId: String,
    public val event: RealmEvent,
)
