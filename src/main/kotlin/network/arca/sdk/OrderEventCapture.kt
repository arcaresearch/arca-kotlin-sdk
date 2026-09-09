package network.arca.sdk

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
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
    private val identityLock = ReentrantLock()
    private var learnedOrderId: String? = null
    private var operation: Operation? = null
    private var objectId: String? = null
    val events = MutableSharedFlow<RealmEvent>(replay = 256, extraBufferCapacity = 256, onBufferOverflow = BufferOverflow.DROP_OLDEST)
    private val consumer = scope.launch(start = CoroutineStart.UNDISPATCHED) {
        ws.orderExecutionEvents().collect { raw ->
            val event = raw.operation?.let { raw.copy(operation = mapOperation(it)) } ?: raw
            events.emit(event)
            val terminal = identityLock.withLock { learnIdentity(event); events.replayCache.any { terminal(it) } }
            if (terminal) stop()
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
        val terminal = identityLock.withLock {
            val projected = mapOperation(operation)
            this.objectId = objectId; this.operation = projected
            learnedOrderId = orderIdentity(projected.outcome)
            events.replayCache.forEach { learnIdentity(it) }
            OrderExecutionReceipt.from(projected, objectId) != null || projected.state in setOf(OperationState.FAILED, OperationState.EXPIRED) || events.replayCache.any { terminal(it) }
        }
        if (terminal) stop()
    }
    private fun orderIdentity(outcome: String?): String? {
        if (outcome.isNullOrEmpty()) return null
        val value = runCatching { arcaJson.parseToJsonElement(outcome) }.getOrNull()
        if (value is JsonObject) return value["orderId"]?.jsonPrimitive?.contentOrNull?.takeIf { it.isNotEmpty() }
        if (value is JsonPrimitive && value.isString) return value.content.takeIf { it.isNotEmpty() }
        return outcome.takeUnless { it.startsWith("{") || it.startsWith("[") }
    }
    private fun scopedOperation(event: RealmEvent): Operation? {
        val op = operation ?: return null
        val account = objectId ?: return null
        val update = event.operation?.takeIf { it.id == op.id } ?: return null
        val scoped = runCatching {
            val observed = update.input?.let { arcaJson.parseToJsonElement(it).jsonObject["exchangeObjectId"]?.jsonPrimitive?.contentOrNull }
            observed == null || observed == account
        }.getOrDefault(false)
        return update.takeIf { scoped }
    }
    private fun learnIdentity(event: RealmEvent) {
        if (learnedOrderId == null) scopedOperation(event)?.let { learnedOrderId = orderIdentity(it.outcome) }
    }
    private fun terminal(event: RealmEvent): Boolean {
        val op = operation ?: return false
        val account = objectId ?: return false
        scopedOperation(event)?.let { update ->
            if (update.state in setOf(OperationState.FAILED, OperationState.EXPIRED)) return true
            val receipt = OrderExecutionReceipt.from(update, account, originalInput = op.input)
            if (receipt != null && (learnedOrderId == null || receipt.orderId == learnedOrderId)) return true
        }
        val id = learnedOrderId ?: return false
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
