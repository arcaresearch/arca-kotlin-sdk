package network.arca.sdk

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
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
    /** The HTTP response (before settlement). */
    public suspend fun submitted(): OrderOperationResponse = inner.submitted()

    /** Wait for full operation settlement (order placement confirmed). */
    public suspend fun settled(): OrderOperationResponse = inner.settled()

    /** Wait for full operation settlement; convenience alias for [settled]. */
    public suspend fun settle(): OrderOperationResponse = inner.settle()

    /** Wait for settlement with an explicit timeout. */
    public suspend fun settled(timeoutSeconds: Double): OrderOperationResponse = inner.settled(timeoutSeconds)

    /**
     * Wait for the order to be fully filled. Re-fetches the order on each
     * inbound fill, returning it with all fills once the status is terminal
     * with fills. Throws if the order reaches a terminal state without fills.
     */
    public suspend fun filled(timeoutSeconds: Double = 30.0): SimOrderWithFills {
        inner.settled()
        val orderId = resolveOrderId()

        return try {
            withTimeout((timeoutSeconds * 1000).toLong()) {
                val initial = deps.getOrder(objectId, orderId)
                if (initial.order.isTerminalWithFills) return@withTimeout initial
                throwIfTerminalWithoutFills(initial.order, orderId)

                deps.fillEvents()
                    .map { deps.getOrder(objectId, orderId) }
                    .firstOrTerminal(orderId)
            }
        } catch (_: TimeoutCancellationException) {
            throw ArcaException.Unknown("TIMEOUT", "Order fill timed out after ${timeoutSeconds.toInt()}s", null)
        }
    }

    private suspend fun Flow<SimOrderWithFills>.firstOrTerminal(orderId: String): SimOrderWithFills {
        var result: SimOrderWithFills? = null
        try {
            first { detail ->
                throwIfTerminalWithoutFills(detail.order, orderId)
                if (detail.order.isTerminalWithFills) {
                    result = detail
                    true
                } else {
                    false
                }
            }
        } catch (_: NoSuchElementException) {
            throw ArcaException.Unknown("STREAM_ENDED", "Fill event stream ended before order was filled", null)
        }
        return result ?: throw ArcaException.Unknown("STREAM_ENDED", "Fill event stream ended before order was filled", null)
    }

    /**
     * A stream of fills as they arrive via WebSocket. The stream closes once the
     * order reaches a terminal status, or after [timeoutSeconds] elapses (which
     * throws [ArcaException.Unknown] with code `TIMEOUT`).
     */
    public fun fills(timeoutSeconds: Double = 300.0): Flow<SimFill> = flow {
        try {
            withTimeout((timeoutSeconds * 1000).toLong()) {
                val response = inner.submitted()
                val orderId = extractOrderId(response.operation.outcome)
                val cloid = extractCloid(response.operation.outcome)

                deps.fillEvents().collect { (fill, _) ->
                    if (fillMatches(fill, orderId, cloid)) {
                        emit(fill)
                        val detail = deps.getOrder(objectId, orderId)
                        val status = detail.order.status
                        if (status == OrderStatus.FILLED || status == OrderStatus.CANCELLED || status == OrderStatus.FAILED) {
                            throw StopCollecting
                        }
                    }
                }
            }
        } catch (_: StopCollecting) {
            // Normal completion: terminal status reached.
        } catch (_: TimeoutCancellationException) {
            throw ArcaException.Unknown("TIMEOUT", "Fill stream timed out after ${timeoutSeconds.toInt()}s", null)
        }
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
        val job = scope.launch {
            runCatching {
                val response = inner.submitted()
                val orderId = extractOrderId(response.operation.outcome)
                val cloid = extractCloid(response.operation.outcome)
                deps.fillEvents().collect { (fill, _) ->
                    if (fillMatches(fill, orderId, cloid)) callback(fill)
                }
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
            order.status == OrderStatus.CANCELLED && (order.filledSize == "0" || order.filledSize.isEmpty()) ->
                throw ArcaException.Unknown("ORDER_${order.status.wire}", "Order $orderId was cancelled with no fills", null)
        }
    }

    private companion object {
        private fun extractOrderId(outcome: String?): String {
            val raw = outcome?.takeIf { it.isNotEmpty() }
                ?: throw ArcaException.Unknown("NO_ORDER_ID", "Operation outcome does not contain an order ID", null)
            val parsed = runCatching {
                arcaJson.parseToJsonElement(raw).jsonObject["orderId"]?.jsonPrimitive?.contentOrNull
            }.getOrNull()
            return parsed?.takeIf { it.isNotEmpty() } ?: raw
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
