package network.arca.sdk

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.*

/** Serializes late venue identity with already-buffered, account-scoped evidence. */
internal class OrderExecutionEvidence(
    private val operation: Operation,
    private val objectId: String,
    private var orderId: String?,
) {
    private val mutex = Mutex()
    private val early = ArrayDeque<RealmEvent>()

    suspend fun orderId(): String? = mutex.withLock { orderId }
    suspend fun receive(event: RealmEvent): OrderExecutionReceipt? = mutex.withLock { receiveLocked(event) }

    private fun belongs(candidate: Operation): Boolean = candidate.id == operation.id && runCatching {
        val account = candidate.input?.let { arcaJson.parseToJsonElement(it).jsonObject["exchangeObjectId"]?.jsonPrimitive?.contentOrNull }
        account == null || account == objectId
    }.getOrDefault(false)

    private fun receiveLocked(event: RealmEvent): OrderExecutionReceipt? {
        event.operation?.takeIf(::belongs)?.let { candidate ->
            throwIfOperationFailed(candidate)
            OrderExecutionReceipt.from(candidate, objectId, originalInput = operation.input)
                ?.takeIf { orderId == null || it.orderId == orderId }?.let { return it }
            if (orderId == null) {
                val learned = runCatching { candidate.outcome?.let { arcaJson.parseToJsonElement(it).jsonObject["orderId"]?.jsonPrimitive?.contentOrNull } }.getOrNull()
                if (!learned.isNullOrEmpty()) {
                    orderId = learned
                    replayLocked()?.let { return it }
                }
            }
        }
        val update = event.order?.order ?: return null
        if (event.entityId != objectId) return null
        if (orderId == null) {
            if (early.size == 256) early.removeFirst()
            early.addLast(event)
            return null
        }
        if ((update.orderId ?: update.id) != orderId) return null
        return OrderExecutionReceipt.from(operation, objectId, update)
    }

    suspend fun snapshot(detail: SimOrderWithFills): OrderExecutionReceipt? = mutex.withLock {
        if (orderId != null && orderId != detail.order.id.value) return@withLock null
        orderId = detail.order.id.value
        replayLocked() ?: OrderExecutionReceipt.from(operation, objectId,
            OrderExecutionUpdate.Value(id = detail.order.id.value, status = detail.order.status.wire,
                filledSize = detail.order.filledSize, avgFillPrice = detail.order.avgFillPrice))
    }

    private fun replayLocked(): OrderExecutionReceipt? {
        val buffered = early.toList()
        early.clear()
        for (event in buffered) receiveLocked(event)?.let { return it }
        return null
    }
}
