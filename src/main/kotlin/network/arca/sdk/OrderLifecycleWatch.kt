package network.arca.sdk

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.ensureActive
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import network.arca.sdk.internal.arcaJson
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.coroutines.coroutineContext

/** A complete server projection, or an explicit loss of current evidence. */
public data class OrderLifecycleUpdate(
    public val lifecycle: OrderLifecycle? = null,
    public val unavailable: Boolean = false,
    public val recoverable: Boolean = false,
    public val reason: String? = null,
)

/** Read-only attachment to one original order. Collect [updates] once; a slow
 * collector receives the latest complete projection. Cancellation stops it. */
public class OrderLifecycleWatch internal constructor(
    channel: Channel<OrderLifecycleUpdate>,
    private val cancel: () -> Unit,
) : AutoCloseable {
    public val updates: Flow<OrderLifecycleUpdate> = channel.receiveAsFlow().onCompletion { stop() }
    public fun stop(): Unit = cancel()
    override fun close(): Unit = stop()
}

@Serializable
private data class LifecycleFrame(
    val type: String,
    val requestId: String,
    val watchId: String? = null,
    val realmId: String? = null,
    val objectId: String? = null,
    val operationId: String? = null,
    val leg: String? = null,
    val lifecycle: OrderLifecycle? = null,
    val unavailable: Boolean = false,
    val recoverable: Boolean = false,
    val reason: String? = null,
    val message: String? = null,
)

// All state shares the manager's lock. Recovery has only a WS send port and
// cannot dispatch orders, store fills, or derive new accounting evidence.
internal class OrderLifecycleTransport(
    private val lock: ReentrantLock,
    private val scope: CoroutineScope,
    private val realm: String,
    private val connected: () -> Boolean,
    private val send: (JsonObject) -> Unit,
    private val sequence: (Int) -> Unit,
    private val interestChanged: (Boolean) -> Unit,
) {
    private class Entry(val objectId: String, val operationId: String, val leg: Int, val timeoutMs: Long) {
        val updates = Channel<OrderLifecycleUpdate>(Channel.CONFLATED)
        var requestId = ""
        var original: OrderLifecycleIntent? = null
        var deadline: Job? = null
        var retry: Job? = null
        var retryMs = 250L
    }
    private val entries = HashMap<String, Entry>()

    fun hasInterest(): Boolean = lock.withLock { entries.isNotEmpty() }

    fun watch(objectId: String, operationId: String, leg: Int, timeoutMs: Long): OrderLifecycleWatch = lock.withLock {
        require(entries.size < 16) { "At most 16 original order watches may be attached" }
        val id = "order-lifecycle-" + UUID.randomUUID()
        val entry = Entry(objectId, operationId, leg, timeoutMs)
        entries[id] = entry
        interestChanged(true)
        attach(id)
        OrderLifecycleWatch(entry.updates) { stop(id) }
    }

    private fun stop(id: String): Unit = lock.withLock {
        val entry = entries.remove(id) ?: return
        entry.deadline?.cancel(); entry.retry?.cancel()
        send(buildJsonObject { put("action", "unwatch_order_lifecycle"); put("watchId", id) })
        entry.updates.close()
        interestChanged(false)
    }

    fun stopAll(): Unit = lock.withLock { entries.keys.toList().forEach { stop(it) } }
    fun reattach(): Unit = lock.withLock { entries.keys.toList().forEach { attach(it) } }
    fun recover(): Unit = lock.withLock { entries.keys.toList().forEach { schedule(it, 0) } }

    private fun attach(id: String) {
        val entry = entries[id] ?: return
        entry.deadline?.cancel(); entry.retry?.cancel(); entry.retry = null
        val requestId = "$id/${UUID.randomUUID()}"
        entry.requestId = requestId
        entry.deadline = scope.launch {
            delay(entry.timeoutMs)
            lock.withLock {
                if (entries[id]?.requestId == requestId) unavailable(id, "snapshot_timeout", true)
            }
        }
        if (!connected()) return
        send(buildJsonObject {
            put("action", "watch_order_lifecycle"); put("watchId", id); put("requestId", requestId)
            put("objectId", entry.objectId); put("operationId", entry.operationId); put("leg", entry.leg.toString())
        })
    }

    private fun schedule(id: String, delayMs: Long) {
        val entry = entries[id] ?: return
        if (entry.retry != null) return
        val requestId = entry.requestId
        // Assign before starting so even a zero-delay gap recovery coalesces.
        val job = scope.launch(start = CoroutineStart.LAZY) {
            delay(delayMs)
            lock.withLock { if (entries[id]?.requestId == requestId) attach(id) }
        }
        entry.retry = job
        job.start()
    }

    private fun unavailable(id: String, reason: String?, recoverable: Boolean) {
        val entry = entries[id] ?: return
        entry.deadline?.cancel(); entry.deadline = null
        entry.updates.trySend(OrderLifecycleUpdate(unavailable = true, recoverable = recoverable, reason = reason))
        if (recoverable) {
            schedule(id, entry.retryMs)
            entry.retryMs = minOf(entry.retryMs * 2, 10_000)
        } else stop(id)
    }

    fun deliver(obj: JsonObject): Boolean = lock.withLock {
        val kind = (obj["type"] as? JsonPrimitive)?.contentOrNull ?: ""
        val requestId = (obj["requestId"] as? JsonPrimitive)?.contentOrNull ?: ""
        if (kind !in listOf("order.lifecycle.updated", "order_lifecycle_watch_created") &&
            !(kind == "error" && requestId.startsWith("order-lifecycle-"))) return false
        (obj["deliverySeq"] as? JsonPrimitive)?.intOrNull?.let(sequence)
        // Old request errors are consumed without disconnecting the new watch.
        val (id, entry) = entries.entries.firstOrNull { it.value.requestId == requestId } ?: return true
        val frame = runCatching { arcaJson.decodeFromJsonElement(LifecycleFrame.serializer(), obj) }.getOrNull()
        if (frame == null) { unavailable(id, "invalid_order_evidence", false); return true }
        if (kind == "order_lifecycle_watch_created") return true // ACK carries no evidence.
        if (kind == "error") { unavailable(id, frame.message, false); return true }
        if (frame.watchId != id || frame.realmId != realm || frame.objectId != entry.objectId ||
            frame.operationId != entry.operationId || frame.leg != entry.leg.toString()) return true
        if (frame.unavailable) { unavailable(id, frame.reason, frame.recoverable); return true }
        val view = frame.lifecycle
        if (view == null || runCatching { view.validate(realm, entry.objectId, entry.operationId, entry.leg) }.isFailure) {
            unavailable(id, "invalid_order_evidence", false); return true
        }
        if (entry.original != null && entry.original != view.intent) {
            unavailable(id, "original_intent_changed", false); return true
        }
        entry.original = view.intent
        entry.deadline?.cancel(); entry.deadline = null
        entry.retryMs = 250
        // Do not cancel recovery already required by a gap.
        entry.updates.trySend(OrderLifecycleUpdate(lifecycle = view))
        true
    }
}

/** Resolve an original path once, then reattach by immutable operation ID and
 * leg after loss. This never repeats placement. */
public suspend fun Arca.watchOrderLifecycle(objectId: String, operation: OriginalOrderReference, leg: Int = 0): OrderLifecycleWatch {
    require(objectId.isNotEmpty() && objectId.length <= 128 && leg >= 0) { "An account and a nonnegative original order leg are required" }
    val operationId = when (operation) {
        is OriginalOrderReference.Id -> operation.value
        is OriginalOrderReference.Path -> getOrderLifecycle(objectId, operation, leg).intent.operationId
    }
    require(operationId.isNotEmpty() && operationId.length <= 128) { "Original operation ID is required" }
    coroutineContext.ensureActive()
    val watch = ws.watchOrderLifecycle(objectId, operationId, leg)
    try { coroutineContext.ensureActive(); return watch }
    catch (e: kotlinx.coroutines.CancellationException) { watch.stop(); throw e }
}
