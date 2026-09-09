package network.arca.sdk

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.selects.select
import network.arca.sdk.models.OrderExecutionReceipt
import network.arca.sdk.models.OrderExecutionUpdate
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import java.util.concurrent.atomic.AtomicLong
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.Fill
import network.arca.sdk.models.FillListResponse
import network.arca.sdk.models.Operation
import network.arca.sdk.models.OrderOperationResponse
import network.arca.sdk.models.OrderStatus
import network.arca.sdk.models.RealmEvent
import network.arca.sdk.models.SimFill
import network.arca.sdk.models.SimOrder
import network.arca.sdk.models.SimOrderWithFills

/** Dependencies injected into [OrderHandle] from the [Arca] client. */
public class OrderHandleDeps internal constructor(
    internal val getOrder: suspend (objectId: String, orderId: String) -> SimOrderWithFills,
    internal val fillEvents: () -> Flow<Pair<SimFill, RealmEvent>>,
    internal val cancelOrder: (cancelPath: String, objectId: String, orderId: String) -> OperationHandle<OrderOperationResponse>,
    internal val modifyOrder: (modifyPath: String, objectId: String, orderId: String, newSize: String) -> OperationHandle<OrderOperationResponse>,
    internal val waitForSettlement: suspend (operationId: String) -> Operation,
    internal val listFills: suspend (objectId: String) -> FillListResponse,
    internal val executionEvents: (() -> Flow<RealmEvent>)? = null,
    internal val releaseExecution: (() -> Unit)? = null,
    internal val awaitExecutionReady: (suspend () -> Unit)? = null,
    internal val getExecutionOperation: (suspend (String) -> Operation)? = null,
    internal val executionGaps: (() -> Flow<Unit>)? = null,
    internal val recoverExecutionReady: (suspend () -> Unit)? = null,
)

/**
 * Handle for exchange order lifecycle. Extends the [OperationHandle] pattern
 * with order-specific methods for waiting on fills, streaming fills, and
 * cancelling.
 *
 * ```kotlin
 * val order = arca.placeOrder(path = "/op/order/btc-1", objectId = id, ...)
 * order.settle() // wait for placement
 *
 * val filled = order.filled(timeoutSeconds = 30.0)
 *
 * order.fills().collect { fill -> println("Filled ${fill.size} @ ${fill.price}") }
 *
 * order.cancel().settle()
 * ```
 */
public class OrderHandle internal constructor(
    private val scope: CoroutineScope,
    private val inner: OperationHandle<OrderOperationResponse>,
    private val objectId: String,
    private val placementPath: String,
    private val deps: OrderHandleDeps,
) {
    @Volatile private var executionDetail: SimOrderWithFills? = null

    /** The HTTP response (before settlement). */
    public suspend fun submitted(): OrderOperationResponse = inner.submitted()

    /** Wait for full operation settlement (order placement confirmed). */
    public suspend fun settled(): OrderOperationResponse = inner.settled()

    /** Wait for full operation settlement; convenience alias for [settled]. */
    public suspend fun settle(): OrderOperationResponse = inner.settle()

    /** Wait for settlement with an explicit timeout. */
    public suspend fun settled(timeoutSeconds: Double): OrderOperationResponse = inner.settled(timeoutSeconds)

    /** Prompt terminal evidence; full metadata and ledger history remain separate. */
    public suspend fun executionReceipt(timeoutSeconds: Double = 30.0): OrderExecutionReceipt = try {
        withTimeout((timeoutSeconds * 1000).toLong()) {
            coroutineScope {
                val submitted = inner.submitted()
                val operation = submitted.operation
                throwIfOperationFailed(operation)
                OrderExecutionReceipt.from(operation, objectId)?.let { return@coroutineScope it }
                val orderId = runCatching { extractOrderId(operation.outcome, allowStructuredFallback = false) }.getOrNull()
                val evidence = OrderExecutionEvidence(operation, objectId, orderId)
                val requests = Channel<Long>(Channel.CONFLATED)
                val revision = AtomicLong()
                requests.trySend(0)
                val gaps = launch(start = CoroutineStart.UNDISPATCHED) {
                    deps.executionGaps?.invoke()?.collect { requests.trySend(revision.incrementAndGet()) }
                }
                val pushed = async(start = CoroutineStart.UNDISPATCHED) {
                    deps.executionEvents?.invoke()?.map { evidence.receive(it) }?.first { it != null } ?: awaitCancellation()
                }
                val snapshot = async {
                    var attempts = 0
                    var consumedRevision = -1L
                    for (requestedRevision in requests) {
                        if (requestedRevision <= consumedRevision) continue
                        var retry = true
                        while (retry && attempts < 3) {
                            attempts++
                            retry = false
                            try {
                                if (attempts == 1) deps.awaitExecutionReady?.invoke() else deps.recoverExecutionReady?.invoke()
                                currentCoroutineContext().ensureActive()
                                consumedRevision = revision.get()
                                if (evidence.orderId() == null && deps.getExecutionOperation != null) {
                                    val recovered = deps.getExecutionOperation.invoke(operation.id.value)
                                    evidence.receive(RealmEvent(type = "operation.updated", operation = recovered))?.let { return@async it }
                                    if (evidence.orderId() == null) break // Healthy pending: wait for push.
                                }
                                val detail = deps.getOrder(objectId, evidence.orderId() ?: operation.id.value)
                                val receipt = evidence.snapshot(detail)
                                if (evidence.orderId() == detail.order.id.value) executionDetail = detail
                                if (receipt != null) return@async receipt
                            } catch (failure: ArcaException.OperationFailed) {
                                throw failure
                            } catch (_: Exception) {
                                currentCoroutineContext().ensureActive()
                                retry = attempts < 3
                            }
                            if (retry) delay(250L * attempts)
                        }
                    }
                    awaitCancellation()
                }
                try { select { pushed.onAwait { it!! }; snapshot.onAwait { it } } }
                finally { pushed.cancel(); snapshot.cancel(); gaps.cancel(); requests.close() }
            }
        }.also { deps.releaseExecution?.invoke() }
    } catch (failure: ArcaException.OperationFailed) {
        deps.releaseExecution?.invoke()
        throw failure
    } catch (_: TimeoutCancellationException) {
        throw ArcaException.Unknown("TIMEOUT", "Order execution timed out", null)
    }

    /** Resolves execution, then returns complete order metadata with available fill history. */
    public suspend fun filled(timeoutSeconds: Double = 30.0): SimOrderWithFills {
        val receipt = executionReceipt(timeoutSeconds)
        val cached = executionDetail
        val detail = if (cached?.order?.id?.value == receipt.orderId && cached.order.isTerminalWithFills) cached
            else deps.getOrder(objectId, receipt.orderId)
        if (detail.order.id.value != receipt.orderId) {
            throw ArcaException.Unknown("ORDER_IDENTITY_MISMATCH", "Order details do not match execution", null)
        }
        throwIfTerminalWithoutFills(detail.order, receipt.orderId)
        if (!detail.order.isTerminalWithFills ||
            detail.order.filledSize.toBigDecimalOrNull()?.compareTo(receipt.filledSize.toBigDecimal()) != 0) {
            throw ArcaException.Unknown("ORDER_DETAILS_PENDING", "Execution completed; full order details are not available yet", null)
        }
        return detail
    }

    /**
     * A stream of fills as they arrive via WebSocket. The stream closes once the
     * order reaches a terminal status, or after [timeoutSeconds] elapses (which
     * throws [ArcaException.Unknown] with code `TIMEOUT`).
     */
    private sealed interface FillMessage {
        data class Execution(val fill: SimFill) : FillMessage
        data class Update(val event: RealmEvent) : FillMessage
    }

    public fun fills(timeoutSeconds: Double = 300.0): Flow<SimFill> = flow {
        try {
            withTimeout((timeoutSeconds * 1000).toLong()) {
                coroutineScope {
                    val queue = kotlinx.coroutines.channels.Channel<FillMessage>(kotlinx.coroutines.channels.Channel.UNLIMITED)
                    val live = launch(start = CoroutineStart.UNDISPATCHED) { deps.fillEvents().collect { queue.send(FillMessage.Execution(it.first)) } }
                    val execution = launch(start = CoroutineStart.UNDISPATCHED) { deps.executionEvents?.invoke()?.collect { queue.send(FillMessage.Update(it)) } }
                    var seed: kotlinx.coroutines.Job? = null
                    try {
                        val response = inner.submitted()
                        throwIfOperationFailed(response.operation)
                        val orderId = extractOrderId(response.operation.outcome)
                        val cloid = extractCloid(response.operation.outcome)
                        var receipt = OrderExecutionReceipt.from(response.operation, objectId)
                        val seen = mutableMapOf<String, SimFill>()
                        if (receipt?.let { fillTotalMatches(seen.values, it.filledSize) } == true) return@coroutineScope
                        seed = launch {
                            val detail = try { deps.getOrder(objectId, orderId) }
                                catch (cancelled: CancellationException) { throw cancelled }
                                catch (_: Exception) { null }
                            if (detail?.order?.id?.value == orderId) {
                                for (fill in detail.fills) queue.send(FillMessage.Execution(fill))
                                queue.send(FillMessage.Update(RealmEvent(type = "order.updated", entityId = objectId,
                                    order = OrderExecutionUpdate(OrderExecutionUpdate.Value(id = orderId, status = detail.order.status.wire, filledSize = detail.order.filledSize, avgFillPrice = detail.order.avgFillPrice)))))
                            }
                        }
                        for (message in queue) {
                            when (message) {
                                is FillMessage.Execution -> {
                                    val fill = message.fill
                                    val key = fill.fillId ?: fill.id.value
                                    if (!fill.isOptimistic && key.isNotEmpty() && fillMatches(fill, orderId, cloid) && !seen.containsKey(key)) {
                                        seen[key] = fill
                                        emit(fill)
                                    }
                                }
                                is FillMessage.Update -> {
                                    val event = message.event
                                    if (event.operation?.id == response.operation.id) {
                                        OrderExecutionReceipt.from(event.operation, objectId, originalInput = response.operation.input)?.takeIf { it.orderId == orderId }?.let { receipt = it }
                                    }
                                    val update = event.order?.order
                                    if (event.entityId == objectId && (update?.orderId ?: update?.id) == orderId) {
                                        OrderExecutionReceipt.from(response.operation, objectId, update)?.let { receipt = it }
                                    }
                                }
                            }
                            if (receipt?.let { fillTotalMatches(seen.values, it.filledSize) } == true) return@coroutineScope
                        }
                    } finally { live.cancel(); execution.cancel(); seed?.cancel(); queue.cancel() }
                }
            }
        } catch (_: TimeoutCancellationException) {
            throw ArcaException.Unknown("TIMEOUT", "Fill stream timed out", null)
        }
    }

    private fun fillTotalMatches(fills: Collection<SimFill>, executed: String): Boolean {
        val expected = OrderExecutionReceipt.decimal(executed) ?: return false
        var total = java.math.BigDecimal.ZERO
        for (fill in fills) total = total.add(OrderExecutionReceipt.decimal(fill.size) ?: return false)
        return total.compareTo(expected) == 0
    }

    /**
     * The platform-side fill record for this order (P&L, fee breakdown,
     * direction, resulting position). Waits for the order to be filled first.
     */
    public suspend fun fillSummary(timeoutSeconds: Double = 30.0): Fill? {
        val result = filled(timeoutSeconds)
        val response = inner.submitted()
        val opId = response.operation.id.value
        val fills = deps.listFills(objectId)
        return fills.fills.firstOrNull { it.operationId == opId || it.orderId == result.order.id.value }
    }

    /** Callback-based fill listener. Returns a cancellation closure. */
    public fun onFill(callback: (SimFill) -> Unit): () -> Unit {
        val job = scope.launch(start = CoroutineStart.UNDISPATCHED) {
            coroutineScope {
                val queued = kotlinx.coroutines.channels.Channel<Pair<SimFill, RealmEvent>>(kotlinx.coroutines.channels.Channel.UNLIMITED)
                val collector = launch(start = CoroutineStart.UNDISPATCHED) {
                    try { deps.fillEvents().collect { queued.send(it) } }
                    finally { queued.close() }
                }
                try {
                    val response = inner.submitted()
                    val orderId = extractOrderId(response.operation.outcome)
                    val cloid = extractCloid(response.operation.outcome)
                    val seen = mutableSetOf<String>()
                    for ((fill, _) in queued) {
                        val key = fill.fillId ?: fill.id.value
                        if (!fill.isOptimistic && key.isNotEmpty() && fillMatches(fill, orderId, cloid) && seen.add(key)) callback(fill)
                    }
                } catch (cancelled: CancellationException) { throw cancelled }
                catch (_: Exception) { /* callback observers close on submission failure */ }
                finally { collector.cancel(); queued.cancel() }
            }
        }
        return { job.cancel() }
    }

    /**
     * Cancel the order. [path] defaults to `<placementPath>/cancel`.
     */
    public fun cancel(path: String? = null): OperationHandle<OrderOperationResponse> {
        val cancelPath = path ?: "$placementPath/cancel"
        return OperationHandle(
            scope = scope,
            submit = {
                val response = inner.submitted()
                val orderId = extractOrderId(response.operation.outcome)
                deps.cancelOrder(cancelPath, objectId, orderId).submitted()
            },
            waitForSettlement = deps.waitForSettlement,
        )
    }

    /**
     * Resize the order to a new total size. Only sized orders can be resized.
     * [path] defaults to the placement path with `/op/order/` replaced by
     * `/op/modify/`, then `-<newSize>` appended.
     */
    public fun resize(newSize: String, path: String? = null): OperationHandle<OrderOperationResponse> {
        val modifyPath = path ?: (placementPath.replace("/op/order/", "/op/modify/") + "-$newSize")
        return OperationHandle(
            scope = scope,
            submit = {
                val response = inner.submitted()
                val orderId = extractOrderId(response.operation.outcome)
                deps.modifyOrder(modifyPath, objectId, orderId, newSize).submitted()
            },
            waitForSettlement = deps.waitForSettlement,
        )
    }

    private suspend fun resolveOrderId(): String {
        val response = inner.settled()
        return extractOrderId(response.operation.outcome)
    }

    private fun throwIfTerminalWithoutFills(order: SimOrder, orderId: String) {
        when {
            order.status == OrderStatus.FAILED ->
                throw ArcaException.Unknown("ORDER_${order.status.wire}", "Order $orderId reached ${order.status.wire}", null)
            order.status == OrderStatus.CANCELLED && (order.filledSize.toBigDecimalOrNull()?.signum() == 0) ->
                throw ArcaException.Unknown("ORDER_${order.status.wire}", "Order $orderId was cancelled with no fills", null)
        }
    }

    private companion object {
        private fun extractOrderId(outcome: String?, allowStructuredFallback: Boolean = true): String {
            val raw = outcome?.takeIf { it.isNotEmpty() }
                ?: throw ArcaException.Unknown("NO_ORDER_ID", "Operation outcome does not contain an order ID", null)
            val parsed = runCatching {
                arcaJson.parseToJsonElement(raw).jsonObject["orderId"]?.jsonPrimitive?.contentOrNull
            }.getOrNull()
            parsed?.takeIf { it.isNotEmpty() }?.let { return it }
            if (!allowStructuredFallback && raw.trimStart().firstOrNull() in setOf('{', '[')) {
                throw ArcaException.Unknown("NO_ORDER_ID", "Operation outcome has no venue order ID yet", null)
            }
            return raw
        }

        /**
         * The order's client id (Hyperliquid cloid) from the placement outcome.
         * A `normalTpsl` bracket child is not a live venue order until the entry
         * fills and the venue arms it — until then it has NO venue order id
         * (extractOrderId falls back to the raw outcome) and is addressable only
         * by its cloid, so fill matching must also key on it. Returns null when
         * the outcome carries no cloid (e.g. sim orders).
         */
        private fun extractCloid(outcome: String?): String? {
            val raw = outcome?.takeIf { it.isNotEmpty() } ?: return null
            return runCatching {
                arcaJson.parseToJsonElement(raw).jsonObject["cloid"]?.jsonPrimitive?.contentOrNull
            }.getOrNull()?.takeIf { it.isNotEmpty() }
        }

        /**
         * Whether a fill belongs to this order. Matches on the venue order id
         * when the order is live, OR on the cloid — the latter is the only
         * handle a still-pending bracket child has before the venue assigns it
         * an oid.
         */
        private fun fillMatches(fill: SimFill, orderId: String, cloid: String?): Boolean {
            if (fill.orderId.value.isNotEmpty() && fill.orderId.value == orderId) return true
            if (!cloid.isNullOrEmpty() && !fill.cloid.isNullOrEmpty() && fill.cloid == cloid) return true
            return false
        }
    }
}

/** Internal control-flow sentinel used to break out of a fill collection loop. */
private object StopCollecting : Throwable() {
    private fun readResolve(): Any = StopCollecting
    override fun fillInStackTrace(): Throwable = this
}
