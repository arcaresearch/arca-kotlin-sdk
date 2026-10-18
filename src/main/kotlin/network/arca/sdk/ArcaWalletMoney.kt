package network.arca.sdk

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.SerializationException
import network.arca.sdk.internal.SseBackoff
import network.arca.sdk.internal.SseParser
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.MoneyDiagnostic
import network.arca.sdk.models.MoneyPrediction
import network.arca.sdk.models.MoneyRevaluation
import network.arca.sdk.models.WALLET_SNAPSHOT_SCHEMA
import network.arca.sdk.models.WalletMoney
import network.arca.sdk.models.WalletSnapshot
import okhttp3.Response
import java.io.IOException
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

// V9 funding: the wallet snapshot, its stream and money figures — one read and
// one stream per owner-facing wallet, composed by Arca from stored records at
// one journal position (documents/contracts/v9-wallet-snapshot.md,
// documents/contracts/v9-wallet-streams.md), and the money figures a product
// renders from them (documents/contracts/sdk-money-figure.md). The realm is
// the one this client was initialised for.

private const val WALLET_SNAPSHOT_PATH = "/custody/v9/funding/wallet-snapshot"
private const val WALLET_STREAM_PATH = "/custody/v9/funding/wallet-stream"

internal fun walletQuery(realm: String, ownerAddress: String, arcaPath: String, boundaryId: String): Map<String, String> {
    val query = mutableMapOf("realmId" to realm, "ownerAddress" to ownerAddress, "arcaPath" to arcaPath)
    if (boundaryId.isNotEmpty()) query["boundaryId"] = boundaryId
    return query
}

/**
 * The complete wallet at one journal position: Cash, every trading account
 * under [arcaPath], money in flight and funding operations. Requires
 * `arca:ReadObject` on the root and the Cash path — a realm-scoped device
 * token is enough. Pass an empty [boundaryId] for a wallet that has
 * registered its funding source but not set up Cash.
 */
public suspend fun Arca.walletSnapshot(ownerAddress: String, arcaPath: String, boundaryId: String): WalletSnapshot {
    val snapshot: WalletSnapshot = client.get(WALLET_SNAPSHOT_PATH, walletQuery(realmId, ownerAddress, arcaPath, boundaryId))
    if (snapshot.schema != WALLET_SNAPSHOT_SCHEMA) {
        throw ArcaException.Unknown("SCHEMA_UNSUPPORTED", "wallet snapshot schema ${snapshot.schema} is not supported by this SDK", null)
    }
    return snapshot
}

/**
 * The money figures of one wallet from a one-shot read:
 * [WalletMoney.compose] of [walletSnapshot]. To follow the wallet, use
 * [watchWalletMoney].
 */
public suspend fun Arca.walletMoney(ownerAddress: String, arcaPath: String, boundaryId: String): WalletMoney =
    WalletMoney.compose(walletSnapshot(ownerAddress, arcaPath, boundaryId))

/**
 * Stream one wallet's snapshots: a complete snapshot on connect and after
 * every change to Cash, a trading account, an operation or a deposit address,
 * coalesced to at most one per 250 ms. Every element is the whole wallet at
 * one journal position — replace what you show.
 *
 * Resume is automatic: after a disconnect the flow reconnects with
 * `Last-Event-ID` set to the last snapshot's `watermark.sequence`, backing
 * off 1 s → 30 s and resetting once a snapshot is delivered, and the server
 * answers with exactly one fresh snapshot. A 401/403 is retried once with a
 * refreshed credential when a token provider is configured. The flow fails
 * only when the server refuses the connection (not found, forbidden, the
 * per-key stream cap); cancelling the collector ends it silently.
 */
public fun Arca.walletSnapshotEvents(ownerAddress: String, arcaPath: String, boundaryId: String): Flow<WalletSnapshot> = channelFlow {
    WalletSnapshotStreamRunner(client, realmId, ownerAddress, arcaPath, boundaryId, log).run(this)
}

/**
 * Watch one wallet's money figures in [scope]: [WalletMoneyWatch.money] is
 * the latest [WalletMoney]. Every figure a product shows — the total, Cash,
 * each account, every leg in motion — comes from the same frame, so figures
 * that changed together in Arca change together on screen.
 *
 * Tell the watch about an accepted operation with [WalletMoneyWatch.predict]
 * so its legs show before the first frame carries it, and give it venue
 * frames with [WalletMoneyWatch.revalue] so an account's figure can tick
 * between snapshots without ever being added to.
 *
 * ```kotlin
 * val watch = arca.watchWalletMoney(scope, owner, root, cash)
 * watch.money.collect { money -> money?.let(::render) }
 * ```
 */
public fun Arca.watchWalletMoney(scope: CoroutineScope, ownerAddress: String, arcaPath: String, boundaryId: String): WalletMoneyWatch {
    val watch = WalletMoneyWatch(scope, log)
    watch.start(walletSnapshotEvents(ownerAddress, arcaPath, boundaryId))
    return watch
}

/**
 * A live view of one wallet's money figures. One instance per wallet; share
 * it across screens so every surface renders the same frame.
 */
public class WalletMoneyWatch internal constructor(
    private val scope: CoroutineScope,
    private val log: ArcaLogger,
) {
    private val stateMut = MutableStateFlow(WatchStreamState.LOADING)
    private val moneyMut = MutableStateFlow<WalletMoney?>(null)
    private val failureMut = MutableStateFlow<Throwable?>(null)

    /** `LOADING` until the first snapshot, then `CONNECTED`. */
    public val state: StateFlow<WatchStreamState> get() = stateMut.asStateFlow()

    /** The latest frame, null before the first snapshot. */
    public val money: StateFlow<WalletMoney?> get() = moneyMut.asStateFlow()

    /** Set when the server refused the stream: the watch is dead and must be recreated. */
    public val failure: StateFlow<Throwable?> get() = failureMut.asStateFlow()

    /** How long a predicted leg is kept when the stream never frames its operation. */
    public var predictionBoundMillis: Long = 300_000

    private val lock = ReentrantLock()
    private var snapshot: WalletSnapshot? = null
    private val predictions = LinkedHashMap<String, MoneyPrediction>()
    private val predictionTimers = HashMap<String, Job>()
    private val revaluations = HashMap<String, MoneyRevaluation>()
    private var job: Job? = null
    private var stopped = false

    internal fun start(events: Flow<WalletSnapshot>) {
        job = scope.launch {
            try {
                events.collect { apply(it) }
            } catch (e: ArcaException) {
                failureMut.value = e
                log.error("stream", e) { "wallet money stream refused" }
            }
        }
    }

    /** Suspends until the first frame has been received. Never throws. */
    public suspend fun ready() {
        state.first { it != WatchStreamState.LOADING }
    }

    /**
     * An accepted operation the server returned, so its legs show at once.
     * It never changes a figure. Retired in the frame that first carries the
     * operation, or at [predictionBoundMillis].
     */
    public fun predict(prediction: MoneyPrediction) {
        lock.withLock {
            predictions[prediction.requestId] = prediction
            predictionTimers.remove(prediction.requestId)?.cancel()
            predictionTimers[prediction.requestId] = scope.launch {
                delay(predictionBoundMillis)
                retirePrediction(prediction.requestId, expired = true)
            }
        }
        recompose()
    }

    /**
     * A venue frame for one account from an exchange-state watch. Used as the
     * account's `revaluedMicro` only when newer than the account's figure and
     * no settlement toward the account is pending; never added to a figure.
     */
    public fun revalue(revaluation: MoneyRevaluation) {
        lock.withLock { revaluations[revaluation.arcaId] = revaluation }
        recompose()
    }

    /** Drop a venue frame (the exchange-state watch for the account ended). */
    public fun clearRevaluation(arcaId: String) {
        lock.withLock { revaluations.remove(arcaId) }
        recompose()
    }

    /** Stop listening. The watch cannot be restarted. */
    public fun stop() {
        val (job, timers) = lock.withLock {
            stopped = true
            val j = job
            job = null
            val t = predictionTimers.values.toList()
            predictionTimers.clear()
            j to t
        }
        job?.cancel()
        timers.forEach { it.cancel() }
    }

    private fun apply(snapshot: WalletSnapshot) {
        lock.withLock {
            if (stopped) return
            if (!snapshot.supersedes(this.snapshot)) {
                log.notice("stream", null, mapOf("sequence" to snapshot.watermark.sequence.toString(), "owner" to snapshot.ownerAddress)) {
                    "ignored a wallet frame that is older or for another wallet"
                }
                return
            }
            this.snapshot = snapshot
            val framed = snapshot.operations.mapNotNull { it.requestIdOrNull }.toSet() + snapshot.operations.map { it.operationId }
            val retired = predictions.filter { (key, p) -> key in framed || (p.operationId != null && p.operationId in framed) }.keys.toList()
            for (key in retired) {
                predictions.remove(key)
                predictionTimers.remove(key)?.cancel()
            }
        }
        recompose()
    }

    private fun retirePrediction(requestId: String, expired: Boolean) {
        val removed = lock.withLock {
            val had = predictions.remove(requestId) != null
            predictionTimers.remove(requestId)
            had
        }
        if (!removed) return
        if (expired) log.notice("stream", null, mapOf("requestId" to requestId)) { "a predicted leg reached its bound without a frame" }
        recompose(if (expired) listOf(MoneyDiagnostic.PredictionExpired(requestId)) else emptyList())
    }

    private fun recompose(extraDiagnostics: List<MoneyDiagnostic> = emptyList()) {
        val composed = lock.withLock {
            if (stopped) return
            val current = snapshot ?: return
            WalletMoney.compose(current, moneyMut.value, predictions.values.toList(), revaluations.toMap())
        }
        val previous = moneyMut.value
        val published = if (extraDiagnostics.isEmpty()) composed else composed.copy(diagnostics = composed.diagnostics + extraDiagnostics)
        for (diagnostic in published.diagnostics) {
            if (previous?.diagnostics?.contains(diagnostic) == true) continue
            log.notice("stream", null, mapOf("detail" to diagnostic.toString(), "sequence" to published.watermark.sequence.toString())) { "wallet money diagnostic" }
        }
        moneyMut.value = published
        if (stateMut.value == WatchStreamState.LOADING) stateMut.value = WatchStreamState.CONNECTED
    }
}

/** The reconnect loop behind [walletSnapshotEvents]. One instance per stream. */
internal class WalletSnapshotStreamRunner(
    private val client: ArcaClient,
    private val realm: String,
    private val ownerAddress: String,
    private val arcaPath: String,
    private val boundaryId: String,
    private val log: ArcaLogger,
) {
    /** Journal position of the last delivered snapshot, sent as `Last-Event-ID`. */
    var lastSequence: Long? = null
        private set

    private var deliveredSinceLastBackoffReset = false

    private sealed class Connection {
        class Disconnected(val cause: Throwable?) : Connection()
        class RefreshAndRetry(val trigger: AuthRefreshTrigger) : Connection()
        class Refused(val error: ArcaException) : Connection()
    }

    suspend fun run(scope: ProducerScope<WalletSnapshot>) {
        var attempt = 0
        var refreshedThisAttempt = false
        while (currentCoroutineContext().isActive) {
            when (val outcome = connect(scope)) {
                is Connection.Refused -> throw outcome.error
                is Connection.RefreshAndRetry -> {
                    if (refreshedThisAttempt || !client.canRefreshToken) {
                        throw if (outcome.trigger == AuthRefreshTrigger.FORBIDDEN) {
                            ArcaException.Forbidden("wallet stream refused", null, "FORBIDDEN")
                        } else {
                            ArcaException.Unauthorized("wallet stream refused", null)
                        }
                    }
                    client.refreshToken(outcome.trigger)
                    refreshedThisAttempt = true
                }
                is Connection.Disconnected -> {
                    if (deliveredSinceLastBackoffReset) {
                        attempt = 0
                        deliveredSinceLastBackoffReset = false
                    }
                    val wait = SseBackoff.delayMillis(attempt)
                    log.notice(
                        "stream",
                        outcome.cause,
                        mapOf("arcaPath" to arcaPath, "lastSequence" to (lastSequence?.toString() ?: ""), "delayMillis" to wait.toString()),
                    ) { "wallet stream disconnected; reconnecting" }
                    attempt += 1
                    refreshedThisAttempt = false
                    delay(wait)
                }
            }
        }
    }

    private suspend fun connect(scope: ProducerScope<WalletSnapshot>): Connection {
        val request = client.streamRequest(WALLET_STREAM_PATH, walletQuery(realm, ownerAddress, arcaPath, boundaryId), lastSequence?.toString())
        val call = client.newStreamCall(request)
        val response: Response = try {
            withContext(Dispatchers.IO) { call.await() }
        } catch (e: IOException) {
            return Connection.Disconnected(ArcaException.Network(e))
        }
        response.use { resp ->
            val body = resp.body ?: return Connection.Disconnected(null)
            if (resp.code != 200) {
                val text = runCatching { body.string() }.getOrDefault("")
                val refusal = client.streamRefusal(text, resp.code)
                return when {
                    refusal is ArcaException.Unauthorized -> Connection.RefreshAndRetry(AuthRefreshTrigger.UNAUTHORIZED)
                    refusal is ArcaException.Forbidden -> Connection.RefreshAndRetry(AuthRefreshTrigger.FORBIDDEN)
                    resp.code in 502..504 -> Connection.Disconnected(refusal)
                    else -> Connection.Refused(refusal)
                }
            }
            val parser = SseParser()
            val source = body.source()
            val buffer = ByteArray(8 * 1024)
            try {
                while (true) {
                    currentCoroutineContext().ensureActive()
                    val read = withContext(Dispatchers.IO) { source.read(buffer) }
                    if (read < 0) break
                    for (i in 0 until read) {
                        val frame = parser.consume(buffer[i].toInt() and 0xff) ?: continue
                        if (frame.event != null && frame.event != "snapshot") continue // position, heartbeat, control: never applied
                        val snapshot = try {
                            arcaJson.decodeFromString(WalletSnapshot.serializer(), frame.data)
                        } catch (e: SerializationException) {
                            return Connection.Refused(ArcaException.Decoding(e))
                        } catch (e: IllegalArgumentException) {
                            return Connection.Refused(ArcaException.Decoding(e))
                        }
                        if (snapshot.schema != WALLET_SNAPSHOT_SCHEMA) {
                            return Connection.Refused(ArcaException.Unknown("SCHEMA_UNSUPPORTED", "wallet snapshot schema ${snapshot.schema} is not supported by this SDK", null))
                        }
                        lastSequence = frame.id?.toLongOrNull() ?: snapshot.watermark.sequence
                        deliveredSinceLastBackoffReset = true
                        scope.send(snapshot)
                    }
                }
            } catch (e: IOException) {
                return Connection.Disconnected(ArcaException.Network(e))
            } finally {
                runCatching { call.cancel() }
            }
        }
        return Connection.Disconnected(null)
    }
}
