package network.arca.sdk

import kotlinx.coroutines.ensureActive

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.isActive
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import network.arca.sdk.models.EventDetailResponse
import network.arca.sdk.models.EventListResponse
import network.arca.sdk.models.NonceResponse
import network.arca.sdk.models.Operation
import network.arca.sdk.models.OperationDetailResponse
import network.arca.sdk.models.OperationListResponse
import network.arca.sdk.models.OperationState
import network.arca.sdk.models.OperationType
import network.arca.sdk.models.ExplorerSummary
import network.arca.sdk.models.StateDeltaListResponse

// MARK: - Operations, events, deltas, nonces, summary

/** Get operation detail by ID (includes correlated events and deltas). */
public suspend fun Arca.getOperation(operationId: String): OperationDetailResponse =
    client.get("/operations/$operationId")

/**
 * List operations in the realm. [types] takes precedence over [type]. When
 * [includeContext] is true, each operation includes its typed context inline.
 */
public suspend fun Arca.listOperations(
    type: OperationType? = null,
    types: List<OperationType>? = null,
    arcaPath: String? = null,
    path: String? = null,
    includeContext: Boolean = false,
): OperationListResponse {
    val query = HashMap<String, String>()
    query["realmId"] = realm
    if (!types.isNullOrEmpty()) {
        query["types"] = types.joinToString(",") { it.value }
    } else if (type != null) {
        query["type"] = type.value
    }
    if (arcaPath != null) query["arcaPath"] = arcaPath
    if (path != null) query["path"] = path
    if (includeContext) query["includeContext"] = "true"
    return client.get("/operations", query)
}

/** List events in the realm. */
public suspend fun Arca.listEvents(arcaPath: String? = null, path: String? = null): EventListResponse {
    val query = HashMap<String, String>()
    query["realmId"] = realm
    if (arcaPath != null) query["arcaPath"] = arcaPath
    if (path != null) query["path"] = path
    return client.get("/events", query)
}

/** Get event detail by ID (includes parent operation and deltas). */
public suspend fun Arca.getEventDetail(eventId: String): EventDetailResponse =
    client.get("/events/$eventId")

/** List state deltas for a given Arca path. */
public suspend fun Arca.listDeltas(arcaPath: String): StateDeltaListResponse =
    client.get("/deltas", mapOf("realmId" to realm, "arcaPath" to arcaPath))

/**
 * Get the next unique nonce for a path. Reserve the nonce *before* the
 * operation and store the resulting path; reuse the stored path on retry.
 */
public suspend fun Arca.nonce(path: String, separator: String? = null): NonceResponse {
    validatePath(path)
    val body = buildJsonObject {
        put("realmId", realm)
        put("prefix", path)
        if (separator != null) put("separator", separator)
    }
    return client.post("/nonce", body = body)
}

/** Get aggregate counts for the realm. */
public suspend fun Arca.summary(): ExplorerSummary =
    client.get("/summary", mapOf("realmId" to realm))

/**
 * Wait for a specific operation to reach a terminal state. Uses WebSocket
 * `operation.updated` events for real-time settlement detection with bounded snapshot recovery on startup and actual stream gaps. Throws [ArcaException.OperationFailed] if the
 * terminal state is `failed` or `expired`.
 */
public suspend fun Arca.waitForOperation(operationId: String, timeoutSeconds: Double = 30.0): Operation =
    waitForSettlement(operationId, timeoutSeconds)

/** Internal WebSocket-based settlement wait used by [OperationHandle]. */
internal suspend fun Arca.waitForSettlement(operationId: String, timeoutSeconds: Double = 30.0): Operation {
    ws.ensureConnected()
    val requests = kotlinx.coroutines.channels.Channel<Long>(kotlinx.coroutines.channels.Channel.CONFLATED)
    val revision = java.util.concurrent.atomic.AtomicLong()
    fun recover() { requests.trySend(revision.incrementAndGet()) }
    val gap = ws.onGap { recover() }
    val auth = ws.onAuthenticated { recover() }
    var acquired = false
    try {
        val result = withTimeoutOrNull<Operation>((timeoutSeconds * 1000).toLong()) {
            coroutineScope {
                // Register the replay-zero collector before acquiring transport interest.
                val pushed = async(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) {
                    ws.events.first { event ->
                        if (event.operation?.id?.value == operationId) event.operation.state.isTerminal
                        else {
                            if (event.operation == null && event.entityId == operationId && event.type in setOf("operation.updated", "operation.created")) recover()
                            false
                        }
                    }.operation!!
                }
                ws.watchPath("/"); acquired = true; recover()
                val snapshots = async<Operation> {
                    var completed = 0L
                    for (requested in requests) {
                        kotlinx.coroutines.currentCoroutineContext().ensureActive()
                        if (requested <= completed) continue
                        var covered = requested
                        for (attempt in 0 until 3) {
                            var acknowledged = false
                            try { kotlinx.coroutines.withTimeout(1000) { ws.recoverPathReady("/") }; acknowledged = true }
                            catch (_: kotlinx.coroutines.TimeoutCancellationException) { /* bounded ACK failure */ }
                            catch (e: CancellationException) { throw e }
                            catch (_: Exception) { /* REST may still recover a terminal result */ }
                            covered = revision.get()
                            try {
                                val operation = getOperation(operationId).operation
                                kotlinx.coroutines.currentCoroutineContext().ensureActive()
                                check(operation.id.value == operationId) { "Operation recovery identity mismatch" }
                                if (operation.state.isTerminal) return@async operation
                                if (acknowledged) break
                            } catch (e: CancellationException) { throw e }
                            catch (_: Exception) { /* retry only actual read failures */ }
                            if (attempt < 2) delay((attempt + 1) * 100L)
                        }
                        completed = covered
                    }
                    throw CancellationException("Operation recovery stopped")
                }
                try { select<Operation> { pushed.onAwait { it }; snapshots.onAwait { it } } }
                finally { pushed.cancel(); snapshots.cancel() }
            }
        } ?: throw ArcaException.Unknown("TIMEOUT", "Timed out waiting for operation $operationId after ${timeoutSeconds.toInt()}s", null)
        throwIfOperationFailed(result)
        return result
    } finally {
        requests.close()
        ws.removeGapHandler(gap); ws.removeAuthenticatedHandler(auth)
        if (acquired) ws.unwatchPath("/")
    }
}

/** Throws [ArcaException.OperationFailed] when the operation reached a non-success terminal state. */
internal fun throwIfOperationFailed(operation: Operation) {
    when (operation.state) {
        OperationState.FAILED, OperationState.EXPIRED -> throw ArcaException.OperationFailed(operation)
        OperationState.PENDING, OperationState.COMPLETED -> Unit
    }
}
