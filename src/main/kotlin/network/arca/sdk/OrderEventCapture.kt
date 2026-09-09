package network.arca.sdk

import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.flow.channelFlow
import network.arca.sdk.models.executionFill
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.launch
import network.arca.sdk.models.Operation
import network.arca.sdk.models.OrderOperationResponse
import network.arca.sdk.models.OperationState
import network.arca.sdk.models.OrderExecutionReceipt
import network.arca.sdk.internal.arcaJson
import kotlinx.serialization.json.*
import network.arca.sdk.models.RealmEvent

/** UNDISPATCHED establishes the replay-zero source collector before POST starts. */
internal class OrderEventCapture(scope: CoroutineScope, private val ws: WebSocketManager,
    private val mapOperation: (Operation) -> Operation = { it }) {
    @Volatile private var operation: Operation? = null
    @Volatile private var objectId: String? = null
    val events = MutableSharedFlow<RealmEvent>(replay = 256, extraBufferCapacity = 256, onBufferOverflow = BufferOverflow.DROP_OLDEST)
    private val consumer = scope.launch(start = CoroutineStart.UNDISPATCHED) {
        ws.orderExecutionEvents().collect { raw ->
            val event = raw.operation?.let { raw.copy(operation = mapOperation(it)) } ?: raw
            events.emit(event)
            if (terminal(event)) stop()
        }
    }
    init {
        ws.watchPath("/")
        consumer.invokeOnCompletion { ws.unwatchPath("/") }
    }
    suspend fun submit(objectId: String, action: suspend () -> OrderOperationResponse): OrderOperationResponse = try {
        action().also { submitted(it.operation, objectId) }
    } catch (error: Throwable) { stop(); throw error }
    fun submitted(operation: Operation, objectId: String) {
        val projected = mapOperation(operation)
        this.objectId = objectId; this.operation = projected
        if (OrderExecutionReceipt.from(projected, objectId) != null || projected.state in setOf(OperationState.FAILED, OperationState.EXPIRED) || events.replayCache.any { terminal(it) }) stop()
    }
    private fun terminal(event: RealmEvent): Boolean {
        val op = operation ?: return false
        val account = objectId ?: return false
        val id = runCatching { op.outcome?.let { arcaJson.parseToJsonElement(it).jsonObject["orderId"]?.jsonPrimitive?.contentOrNull } }.getOrNull()
        if (event.operation?.id == op.id) {
            val scoped = runCatching {
                val observed = event.operation.input?.let { arcaJson.parseToJsonElement(it).jsonObject["exchangeObjectId"]?.jsonPrimitive?.contentOrNull }
                observed == null || observed == account
            }.getOrDefault(false)
            if (!scoped) return false
            if (event.operation.state in setOf(OperationState.FAILED, OperationState.EXPIRED)) return true
            val receipt = OrderExecutionReceipt.from(event.operation, account, originalInput = op.input)
            if (receipt != null && (id == null || receipt.orderId == id)) return true
        }
        if (id == null) return false
        val update = event.order?.order ?: return false
        return event.entityId == account && (update.orderId ?: update.id) == id && OrderExecutionReceipt.from(op, account, update) != null
    }
    fun fillEvents() = channelFlow {
        val live = launch(start = CoroutineStart.UNDISPATCHED) { ws.fillEvents().collect { send(it) } }
        ws.watchPath("/")
        try {
            for (event in events.replayCache) event.executionFill?.let { send(it to event) }
            awaitCancellation()
        } finally { live.cancel(); ws.unwatchPath("/") }
    }
    suspend fun awaitReady() { ws.awaitPathReady("/") }
    fun stop() { consumer.cancel() }
}
