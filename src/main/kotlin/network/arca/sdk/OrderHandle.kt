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
    /**
     * Platform-recorded fills (`fill.recorded`), the accounting-time event.
     * Optional so partial bundles keep working; without it [OrderHandle.accounted]
     * converges through its bounded reads alone.
     */
    internal val recordedFillEvents: (() -> Flow<Pair<Fill, RealmEvent>>)? = null,
    /**
     * Keep the realm root watched until the returned release runs, so
     * `fill.recorded` frames for this order reach the socket even when the
     * application holds no other watch covering the account.
     */
    internal val holdAccountWatch: (() -> (() -> Unit))? = null,
    /**
     * Tell any live exchange-state watch for the object that its account
     * changed, so it re-reads. Called when accounting completion was learned
     * through a REST read — the moment a lost account push is most likely.
     */
    internal val exchangeStateChanged: ((String) -> Unit)? = null,
)

/** Bounded fallback-read schedule for [OrderHandle.accounted]. */
private const val ACCOUNTED_READ_INITIAL_MS = 500L
private const val ACCOUNTED_READ_MAX_MS = 8_000L

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
    @Volatile private var positionUpdate: PositionUpdate? = null
    @Volatile private var executionDetail: SimOrderWithFills? = null

    /** Bind a baseline captured before submission, including backend-submitted orders. */
    public suspend fun trackPositionUpdate(update: PositionUpdate) {
        update.view.bind(update, inner.submitted().operation, objectId)
        positionUpdate = update
    }

    /**
     * Read-only, retryable server verification of terminal zero execution for this display scope.
     * False or a read error leaves it active. This does not report ledger completion or order success.
     */
    public suspend fun retirePositionUpdateIfNoExecution(): Boolean {
        val update = positionUpdate ?: return false
        val original = inner.submitted().operation
        val operation = deps.getExecutionOperation?.invoke(original.id.value) ?: original
        if (operation.id != original.id) return false
        if (update.view.retireNoExecution(update, operation)) return true
        // The lifecycle endpoint explicitly supports the original operation ID.
        val detail = deps.getOrder(objectId, original.id.value)
        return update.view.retireNoExecution(update, operation, detail)
    }

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
        }.also { receipt ->
            positionUpdate?.let { it.view.receive(it, receipt) }
            deps.releaseExecution?.invoke()
        }
    } catch (failure: ArcaException.OperationFailed) {
        try { retirePositionUpdateIfNoExecution() }
        catch (cancelled: CancellationException) { throw cancelled }
        catch (_: Exception) { /* Preserve the original failure and the unverified scope. */ }
        deps.releaseExecution?.invoke()
        throw failure
    } catch (_: TimeoutCancellationException) {
        throw ArcaException.Unknown("TIMEOUT", "Order execution timed out", null)
    }

    /**
     * Wait until the account reflects this order's execution.
     *
     * [executionReceipt] proves terminal execution at the venue; the ledger
     * commit that updates the account's positions and balances happens
     * afterwards, and an exchange-state read taken in between returns the
     * pre-accounting snapshot. This resolves once every executed quantity is
     * recorded — the platform's `fillsComplete` — so a `getExchangeState` /
     * `watchExchangeState` observation taken after it includes the execution.
     * A live `watchExchangeState` for the account is refreshed when completion
     * had to be learned through a read.
     *
     * Push-first: each `fill.recorded` for this order triggers one order read,
     * as do delivery gaps and reconnects. A lost push is covered by a bounded
     * backoff read (500ms doubling to 8s) until the deadline. On venues whose
     * order read carries no `fillsComplete`, completion is the recorded fills
     * for the order covering its executed size exactly.
     *
     * Resolves for a zero-fill terminal order too (nothing to account). Throws
     * the placement failure, or [ArcaException.Unknown] with code `TIMEOUT`
     * when accounting has not completed within [timeoutSeconds].
     *
     * ```kotlin
     * val receipt = order.executionReceipt()   // show the receipt
     * val detail = order.accounted()           // then trust the account
     * val state = arca.getExchangeState(objectId)
     * ```
     */
    public suspend fun accounted(timeoutSeconds: Double = 30.0): SimOrderWithFills {
        val deadlineMs = System.currentTimeMillis() + (timeoutSeconds * 1000).toLong()
        // Register recorded-fill delivery and hold the root watch BEFORE the
        // receipt, so a fill recorded between the two cannot be missed.
        val recorded = deps.recordedFillEvents?.invoke()
        val release = deps.holdAccountWatch?.invoke()
        try {
            val detail = accountedDetail(deadlineMs, recorded)
            positionUpdate?.let { it.view.accounted(it, detail) }
            return detail
        } finally {
            release?.invoke()
        }
    }

    private suspend fun accountedDetail(deadlineMs: Long, recorded: Flow<Pair<Fill, RealmEvent>>?): SimOrderWithFills {
        val receipt = executionReceipt(maxOf(0L, deadlineMs - System.currentTimeMillis()) / 1000.0)
        val orderId = receipt.orderId
        val operationId = receipt.operationId
        executionDetail?.let { cached ->
            if (cached.order.id.value == orderId && cached.fillsComplete == true) return cached
        }
        val detail = try {
            withTimeout(maxOf(1L, deadlineMs - System.currentTimeMillis())) {
                coroutineScope {
                    val requests = Channel<Unit>(Channel.CONFLATED)
                    requests.trySend(Unit)
                    val pushes = launch(start = CoroutineStart.UNDISPATCHED) {
                        recorded?.collect { (fill, _) ->
                            if (recordedFillMatches(fill, orderId, operationId)) requests.trySend(Unit)
                        }
                    }
                    val gaps = launch(start = CoroutineStart.UNDISPATCHED) {
                        deps.executionGaps?.invoke()?.collect { requests.trySend(Unit) }
                    }
                    // The fallback for a lost push: exchange.updated / fill.recorded
                    // have no durable log, and a deferred enrichment is dropped
                    // without a deliverySeq, so a quiet socket proves nothing.
                    val backoff = launch {
                        var attempt = 0
                        while (true) {
                            delay(minOf(ACCOUNTED_READ_MAX_MS, ACCOUNTED_READ_INITIAL_MS shl attempt))
                            attempt = minOf(attempt + 1, 6)
                            requests.trySend(Unit)
                        }
                    }
                    try {
                        var found: SimOrderWithFills? = null
                        for (request in requests) {
                            currentCoroutineContext().ensureActive()
                            try {
                                val current = deps.getOrder(objectId, orderId)
                                if (current.order.id.value != orderId) continue
                                if (isAccounted(current, orderId, operationId)) { found = current; break }
                            } catch (failure: ArcaException.OperationFailed) {
                                throw failure
                            } catch (cancelled: CancellationException) {
                                throw cancelled
                            } catch (_: Exception) {
                                // Transient read failure: the next trigger re-reads.
                            }
                        }
                        found ?: throw ArcaException.Unknown("STREAM_ENDED", "Order accounting evidence unavailable", null)
                    } finally { pushes.cancel(); gaps.cancel(); backoff.cancel(); requests.close() }
                }
            }
        } catch (_: TimeoutCancellationException) {
            throw ArcaException.Unknown("TIMEOUT", "Order accounting timed out", null)
        }
        // Completion was established by a read. The account push for this
        // commit travels a different path (the mirror relay) from the
        // `fill.recorded` push that may have triggered the read, so seeing one
        // proves nothing about the other: always nudge the account watch. Its
        // re-read coalesces with any push-triggered read already in flight.
        deps.exchangeStateChanged?.invoke(objectId)
        return detail
    }

    /**
     * Completion check with the venue-read fallback: when the order read does
     * not carry the platform's accounting view, the platform-recorded fills
     * for the order must cover its executed size exactly (a zero-fill terminal
     * order is trivially covered).
     */
    private suspend fun isAccounted(detail: SimOrderWithFills, orderId: String, operationId: String): Boolean {
        detail.fillsComplete?.let { return it }
        when (detail.order.status) {
            OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.FAILED -> Unit
            else -> return false
        }
        val executed = detail.order.filledSize
        if (recordedSizesCover(emptyList(), executed)) return true
        val recorded = deps.listFills(objectId)
        val sizes = recorded.fills
            .filter { !it.operationId.isNullOrEmpty() && (it.orderId == orderId || it.orderOperationId == operationId) }
            .mapNotNull { it.size }
        return sizes.isNotEmpty() && recordedSizesCover(sizes, executed)
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

    internal companion object {
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

        /**
         * Whether a platform-recorded fill belongs to this order. Recorded
         * fills carry the venue order id and the placement operation id; a
         * bracket child that was still pending when it filled is matched by
         * the latter.
         */
        internal fun recordedFillMatches(fill: Fill, orderId: String, operationId: String): Boolean {
            if (!fill.orderId.isNullOrEmpty() && fill.orderId == orderId) return true
            if (!fill.orderOperationId.isNullOrEmpty() && fill.orderOperationId == operationId) return true
            return false
        }

        /**
         * Exact decimal comparison: the recorded sizes must sum to the
         * executed size. Sizes cross the wire as decimal strings and never
         * round-trip through a binary float here — `0.1 + 0.2` must cover `0.3`.
         */
        internal fun recordedSizesCover(sizes: List<String>, executed: String): Boolean {
            val expected = OrderExecutionReceipt.decimal(executed) ?: return false
            var total = java.math.BigDecimal.ZERO
            for (raw in sizes) total = total.add(OrderExecutionReceipt.decimal(raw) ?: return false)
            return total.compareTo(expected) == 0
        }
    }
}

/** Internal control-flow sentinel used to break out of a fill collection loop. */
private object StopCollecting : Throwable() {
    private fun readResolve(): Any = StopCollecting
    override fun fillInStackTrace(): Throwable = this
}
