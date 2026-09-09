package network.arca.sdk

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import network.arca.sdk.models.Operation
import network.arca.sdk.models.OperationState
import network.arca.sdk.models.OperationType
import network.arca.sdk.models.OrderOperationResponse
import network.arca.sdk.models.OrderSide
import network.arca.sdk.models.OrderStatus
import network.arca.sdk.models.OrderType
import network.arca.sdk.models.RealmEvent
import network.arca.sdk.models.SimFill
import network.arca.sdk.models.SimOrder
import network.arca.sdk.models.SimOrderWithFills
import network.arca.sdk.models.TimeInForce
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

private fun makeOrderOperation(
    id: String = "op_order_1",
    state: OperationState = OperationState.COMPLETED,
    outcome: String? = "ord_abc",
    input: String? = null,
): Operation = Operation(
    id = OperationId(id),
    realmId = RealmId("rlm_test"),
    path = "/op/order/btc-buy-1",
    type = OperationType.ORDER,
    state = state,
    outcome = outcome,
    input = input,
    actorType = "user",
    createdAt = "2026-03-08T00:00:00.000000Z",
    updatedAt = "2026-03-08T00:00:00.000000Z",
)

private fun makeFill(
    id: String = "fill_1",
    orderId: String = "ord_abc",
    cloid: String? = null,
    size: String = "0.5",
    price: String = "50000",
): SimFill = SimFill(
    id = SimFillId(id),
    orderId = SimOrderId(orderId),
    cloid = cloid,
    accountId = SimAccountId("acc_1"),
    realmId = RealmId("rlm_test"),
    market = "BTC",
    side = OrderSide.BUY,
    price = price,
    size = size,
    fee = "0.50",
    isLiquidation = false,
    createdAt = "2026-03-08T00:00:00.000000Z",
)

private fun makeSimOrder(
    id: String = "ord_abc",
    status: OrderStatus = OrderStatus.FILLED,
    size: String = "1.0",
    filledSize: String = "1.0",
    timeInForce: TimeInForce = TimeInForce.IOC,
): SimOrder = SimOrder(
    id = SimOrderId(id),
    accountId = SimAccountId("acc_1"),
    realmId = RealmId("rlm_test"),
    market = "ETH",
    side = OrderSide.SELL,
    orderType = OrderType.MARKET,
    size = size,
    filledSize = filledSize,
    avgFillPrice = "2000",
    status = status,
    reduceOnly = false,
    timeInForce = timeInForce,
    leverage = 5,
    createdAt = "2026-03-08T00:00:00.000000Z",
    updatedAt = "2026-03-08T00:00:00.000000Z",
)

class OrderHandleTest {

    private lateinit var scope: CoroutineScope

    @BeforeEach
    fun setUp() {
        scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    }

    @AfterEach
    fun tearDown() {
        scope.cancel()
    }

    private fun unexpectedDeps(
        getOrder: suspend (String, String) -> SimOrderWithFills = { _, _ -> error("unexpected") },
        fillEvents: () -> kotlinx.coroutines.flow.Flow<Pair<SimFill, RealmEvent>> = { error("unexpected") },
        cancelOrder: (String, String, String) -> OperationHandle<OrderOperationResponse> = { _, _, _ -> error("unexpected") },
        modifyOrder: (String, String, String, String) -> OperationHandle<OrderOperationResponse> = { _, _, _, _ -> error("unexpected") },
        waitForSettlement: suspend (String) -> Operation = { error("unexpected") },
        listFills: suspend (String) -> network.arca.sdk.models.FillListResponse = { error("unexpected") },
        getExecutionOperation: (suspend (String) -> Operation)? = null,
        executionGaps: (() -> kotlinx.coroutines.flow.Flow<Unit>)? = null,
        awaitExecutionReady: (suspend () -> Unit)? = null,
        recoverExecutionReady: (suspend () -> Unit)? = null,
    ): OrderHandleDeps = OrderHandleDeps(getOrder, fillEvents, cancelOrder, modifyOrder, waitForSettlement, listFills,
        getExecutionOperation = getExecutionOperation, executionGaps = executionGaps,
        awaitExecutionReady = awaitExecutionReady, recoverExecutionReady = recoverExecutionReady)

    private fun orderHandle(inner: OperationHandle<OrderOperationResponse>, placementPath: String, deps: OrderHandleDeps): OrderHandle =
        OrderHandle(scope = scope, inner = inner, objectId = "obj_exchange", placementPath = placementPath, deps = deps)

    @Test
    fun lateIdentityRechecksBufferedEvidenceAndPreservesOriginalIntent() = runBlocking {
        val original = makeOrderOperation(state = OperationState.PENDING, outcome = "{}", input = """{"exchangeObjectId":"obj_exchange","size":"40","timeInForce":"IOC"}""")
        val evidence = OrderExecutionEvidence(original, "obj_exchange", null)
        val event = RealmEvent(type = "order.updated", entityId = "obj_exchange", order = network.arca.sdk.models.OrderExecutionUpdate(network.arca.sdk.models.OrderExecutionUpdate.Value(id = "ord_abc", status = "FILLED", filledSize = "4.041")))
        assertEquals(null, evidence.receive(event))
        assertEquals(null, evidence.receive(RealmEvent(type = "operation.updated", operation = original.copy(state = OperationState.FAILED, input = """{"exchangeObjectId":"foreign"}"""))))
        val learned = makeOrderOperation(outcome = """{"orderId":"ord_abc","status":"OPEN","filledSize":"0"}""", input = """{"exchangeObjectId":"obj_exchange","size":"4.041"}""")
        val receipt = evidence.receive(RealmEvent(type = "operation.updated", operation = learned))!!
        assertEquals("ord_abc", receipt.orderId)
        assertEquals("40", receipt.requestedSize)
        assertEquals("4.041", receipt.filledSize)
    }

    @Test
    fun healthyOpenIsQuietAndActualGapUsesFreshBarrier() = runBlocking {
        val op = makeOrderOperation(outcome = """{"orderId":"ord_abc"}""")
        val inner = OperationHandle(scope, submit = { OrderOperationResponse(operation = op) }, waitForSettlement = { error("unexpected") })
        val reads = java.util.concurrent.atomic.AtomicInteger()
        val fresh = java.util.concurrent.atomic.AtomicInteger()
        val seeded = kotlinx.coroutines.CompletableDeferred<Unit>()
        val gaps = kotlinx.coroutines.flow.MutableSharedFlow<Unit>(extraBufferCapacity = 1)
        val deps = unexpectedDeps(getOrder = { _, _ ->
            val count = reads.incrementAndGet()
            seeded.complete(Unit)
            SimOrderWithFills(order = makeSimOrder(status = if (count == 1) OrderStatus.OPEN else OrderStatus.FILLED), fills = emptyList())
        }, executionGaps = { gaps }, recoverExecutionReady = { fresh.incrementAndGet(); Unit })
        val pending = async { orderHandle(inner, "/order", deps).executionReceipt(1.0) }
        seeded.await()
        kotlinx.coroutines.delay(20)
        assertEquals(1, reads.get())
        gaps.emit(Unit)
        assertEquals("ord_abc", pending.await().orderId)
        assertEquals(2, reads.get())
        assertEquals(1, fresh.get())
    }

    @Test
    fun failedAcknowledgementRecoversOriginalOperationWithoutOrderIdGuess() = runBlocking {
        val op = makeOrderOperation(state = OperationState.PENDING, outcome = "{}", input = """{"exchangeObjectId":"obj_exchange","size":"40"}""")
        val inner = OperationHandle(scope, submit = { OrderOperationResponse(operation = op) }, waitForSettlement = { error("unexpected") })
        val reads = java.util.concurrent.atomic.AtomicInteger()
        val fresh = java.util.concurrent.atomic.AtomicInteger()
        val deps = unexpectedDeps(awaitExecutionReady = { error("disconnected before ACK") }, recoverExecutionReady = { fresh.incrementAndGet(); Unit },
            getExecutionOperation = { id ->
                assertEquals(op.id.value, id)
                reads.incrementAndGet()
                makeOrderOperation(outcome = """{"orderId":"ord_abc","status":"FILLED","filledSize":"4.041"}""")
            })
        val receipt = orderHandle(inner, "/order", deps).executionReceipt(1.0)
        assertEquals("40", receipt.requestedSize)
        assertEquals("4.041", receipt.filledSize)
        assertEquals(1, fresh.get())
        assertEquals(1, reads.get())
    }

    @Test
    fun unavailableRecoveryHasFiniteBudget() = runBlocking {
        val op = makeOrderOperation(outcome = """{"orderId":"ord_abc"}""")
        val inner = OperationHandle(scope, submit = { OrderOperationResponse(operation = op) }, waitForSettlement = { error("unexpected") })
        val reads = java.util.concurrent.atomic.AtomicInteger()
        val deps = unexpectedDeps(getOrder = { _, _ -> reads.incrementAndGet(); error("unavailable") })
        try { orderHandle(inner, "/order", deps).executionReceipt(0.9); error("expected timeout") }
        catch (failure: ArcaException.Unknown) { assertEquals("TIMEOUT", failure.code) }
        assertEquals(3, reads.get())
    }

    @Test
    fun terminalReceiptsKeepOriginalIOCQuantityWithoutOrderRead() = runBlocking {
        for ((requested, executed, remaining) in listOf(Triple("40.110692","4.041","36.069692"), Triple("286.522911","1.809","284.713911"), Triple("47.700441","3.825","43.875441"))) {
            val op = makeOrderOperation(outcome = """{"orderId":"ord_abc","status":"filled","filledSize":"$executed","avgFillPrice":"328"}""", input = """{"exchangeObjectId":"obj_exchange","size":"$requested","gllPrepared":{"request":{"Effect":1}}}""")
            val inner = OperationHandle(scope, submit = { OrderOperationResponse(operation = op) }, waitForSettlement = { error("must not wait for settlement") })
            val receipt = orderHandle(inner,"/order",unexpectedDeps()).executionReceipt(1.0)
            assertEquals(requested,receipt.requestedSize)
            assertEquals(remaining,receipt.remainingSize)
            assertEquals("partial",receipt.fulfillmentState)
            assertEquals("cancelled",receipt.remainingDisposition)
            assertFalse(receipt.averagePriceFinal)
            assertFalse(receipt.fillsComplete)
        }
    }

    @Test
    fun terminalPushWinsBeforeWatchAcknowledgement() = runBlocking {
        val op = makeOrderOperation(state = OperationState.PENDING, outcome = """{"orderId":"ord_abc"}""")
        val inner = OperationHandle(scope, submit = { OrderOperationResponse(operation = op) }, waitForSettlement = { error("must not wait for settlement") })
        val deps = OrderHandleDeps(
            getOrder = { _, _ -> error("read must await ACK") }, fillEvents = { emptyFlow() },
            cancelOrder = { _, _, _ -> error("unused") }, modifyOrder = { _, _, _, _ -> error("unused") },
            waitForSettlement = { error("unused") }, listFills = { error("unused") },
            executionEvents = { flowOf(RealmEvent(type = "order.updated", entityId = "obj_exchange", order = network.arca.sdk.models.OrderExecutionUpdate(network.arca.sdk.models.OrderExecutionUpdate.Value(id = "ord_abc", status = "FILLED", filledSize = "3.825")))) },
            awaitExecutionReady = { kotlinx.coroutines.awaitCancellation() },
        )
        val receipt = orderHandle(inner,"/order",deps).executionReceipt(1.0)
        assertEquals("3.825",receipt.filledSize)
    }

    @Test
    fun timeoutRetainsCaptureForConfirmationRetry() = runBlocking {
        val op = makeOrderOperation(state = OperationState.PENDING, outcome = """{"orderId":"ord_abc"}""")
        val inner = OperationHandle(scope, submit = { OrderOperationResponse(operation = op) }, waitForSettlement = { error("No settlement wait") })
        val attempts = java.util.concurrent.atomic.AtomicInteger()
        val releases = java.util.concurrent.atomic.AtomicInteger()
        val deps = OrderHandleDeps(
            getOrder = { _, _ -> error("Read awaits ACK") }, fillEvents = { emptyFlow() },
            cancelOrder = { _, _, _ -> error("unused") }, modifyOrder = { _, _, _, _ -> error("unused") },
            waitForSettlement = { error("unused") }, listFills = { error("unused") },
            releaseExecution = { releases.incrementAndGet() },
            executionEvents = { kotlinx.coroutines.flow.flow {
                if (attempts.incrementAndGet() == 1) kotlinx.coroutines.awaitCancellation()
                emit(RealmEvent(type = "order.updated", entityId = "obj_exchange", order = network.arca.sdk.models.OrderExecutionUpdate(network.arca.sdk.models.OrderExecutionUpdate.Value(id = "ord_abc", status = "FILLED", filledSize = "3.825"))))
            } },
            awaitExecutionReady = { kotlinx.coroutines.awaitCancellation() },
        )
        val handle = orderHandle(inner,"/order",deps)
        try { handle.executionReceipt(0.01); error("Expected timeout") } catch (_: ArcaException.Unknown) {}
        assertEquals(0, releases.get())
        assertEquals("3.825",handle.executionReceipt(1.0).filledSize)
        assertEquals(1, releases.get())
    }

    @Test
    fun fillStreamDeduplicatesWithoutPerFillReads() = runBlocking {
        val op = makeOrderOperation(outcome = """{"orderId":"ord_abc","status":"filled","filledSize":"1"}""")
        val inner = OperationHandle(scope, submit = { OrderOperationResponse(operation = op) }, waitForSettlement = { error("unused") })
        val values = listOf(makeFill(id="aggregate",size="1").copy(isOptimistic=true), makeFill(id="venue-1",size="0.3"), makeFill(id="ledger-1",size="0.3").copy(fillId="venue-1"), makeFill(id="venue-2",size="0.7"))
        val reads = java.util.concurrent.atomic.AtomicInteger()
        val deps = unexpectedDeps(getOrder = { _, _ -> reads.incrementAndGet(); kotlinx.coroutines.awaitCancellation() }, fillEvents = { flowOf(*values.map { it to RealmEvent(type="fill.previewed") }.toTypedArray()) })
        val received = mutableListOf<String>()
        orderHandle(inner,"/order",deps).fills(1.0).collect { received.add(it.id.value) }
        assertEquals(listOf("venue-1","venue-2"),received)
        assertTrue(reads.get() <= 1)
    }

    @Test
    fun operationUpdateKeepsOriginalRequestedQuantity() {
        val original = """{"exchangeObjectId":"obj_exchange","size":"40.110692","timeInForce":"IOC"}"""
        val updated = makeOrderOperation(outcome = """{"orderId":"ord_abc","status":"FILLED","filledSize":"4.041"}""", input = """{"exchangeObjectId":"obj_exchange","size":"4.041"}""")
        val receipt = network.arca.sdk.models.OrderExecutionReceipt.from(updated,"obj_exchange",originalInput = original)!!
        assertEquals("40.110692",receipt.requestedSize)
        assertEquals("36.069692",receipt.remainingSize)
        assertEquals("partial",receipt.fulfillmentState)
        assertEquals(null,network.arca.sdk.models.OrderExecutionReceipt.from(updated.copy(input = """{"exchangeObjectId":"other","size":"4.041"}"""),"obj_exchange",originalInput = original))
    }

    @Test
    fun terminalVenueStatusAliases() {
        for ((status, canonical, state) in listOf(Triple("REJECTED", "FAILED", "rejected"), Triple("EXPIRED", "CANCELLED", "no_fill"), Triple("CANCELED", "CANCELLED", "no_fill"))) {
            val op = makeOrderOperation(outcome = """{"orderId":"ord_abc","status":"$status","filledSize":"0"}""")
            val receipt = network.arca.sdk.models.OrderExecutionReceipt.from(op,"obj_exchange")!!
            assertEquals(canonical, receipt.status)
            assertEquals(state, receipt.executionState)
            assertEquals("cancelled", receipt.remainingDisposition)
        }
    }

    @Test
    fun receiptUnknownIntentAndDecimalZero() {
        val op = makeOrderOperation(outcome = """{"orderId":"ord_abc","status":"filled","filledSize":"4.041"}""")
        val receipt = network.arca.sdk.models.OrderExecutionReceipt.from(op,"obj_exchange")!!
        assertEquals("unknown",receipt.fulfillmentState)
        assertEquals(null,receipt.requestedSize)
        for (quantity in listOf("0","0.0","0.000")) {
            val zero = makeOrderOperation(outcome = """{"orderId":"ord_abc","status":"cancelled","filledSize":"$quantity"}""")
            assertEquals("no_fill",network.arca.sdk.models.OrderExecutionReceipt.from(zero,"obj_exchange")?.executionState)
        }
        for (quantity in listOf("1/2","0x10","1e2","-1")) assertEquals(null,network.arca.sdk.models.OrderExecutionReceipt.decimal(quantity))
    }

    @Test
    fun settledDelegatesToInner() = runBlocking {
        val op = makeOrderOperation(state = OperationState.COMPLETED)
        val inner = OperationHandle(scope, submit = { OrderOperationResponse(operation = op) }, waitForSettlement = { op })
        val handle = orderHandle(inner, "/op/order/btc-buy-1", unexpectedDeps())
        val result = handle.settled()
        assertEquals(OperationState.COMPLETED, result.operation.state)
    }

    @Test
    fun submittedDelegatesToInner() = runBlocking {
        val op = makeOrderOperation(state = OperationState.PENDING)
        val inner = OperationHandle(
            scope,
            submit = { OrderOperationResponse(operation = op) },
            waitForSettlement = {
                delay(500)
                makeOrderOperation(state = OperationState.COMPLETED)
            },
        )
        val handle = orderHandle(inner, "/op/order/btc-buy-1", unexpectedDeps())
        val submitted = handle.submitted()
        assertEquals(OperationState.PENDING, submitted.operation.state)
    }

    @Test
    fun onFillReceivesMatchingFills() {
        val op = makeOrderOperation(state = OperationState.COMPLETED, outcome = "ord_abc")
        val inner = OperationHandle(scope, submit = { OrderOperationResponse(operation = op) }, waitForSettlement = { op })

        val matchingFill = makeFill(orderId = "ord_abc", size = "0.5")
        val deps = unexpectedDeps(
            fillEvents = { flowOf(matchingFill to RealmEvent(realmId = "rlm_test", type = "fill.previewed")) },
        )
        val handle = orderHandle(inner, "/op/order/btc-buy-1", deps)

        var receivedFill: SimFill? = null
        val latch = CountDownLatch(1)
        val unsub = handle.onFill { fill ->
            receivedFill = fill
            latch.countDown()
        }

        assertTrue(latch.await(2, TimeUnit.SECONDS))
        assertEquals("0.5", receivedFill?.size)
        assertEquals("ord_abc", receivedFill?.orderId?.value)
        unsub()
    }

    @Test
    fun onFillMatchesPendingBracketChildByCloid() {
        // Pending normalTpsl child: outcome carries the cloid but NO venue
        // orderId, so extractOrderId falls back to the raw outcome. Only cloid
        // identity can correlate the fill once the venue arms the child.
        val cloid = "0xdeadbeefdeadbeefdeadbeefdeadbeef"
        val op = makeOrderOperation(
            state = OperationState.COMPLETED,
            outcome = """{"orderId":"","cloid":"$cloid","tpsl":"tp"}""",
        )
        val inner = OperationHandle(scope, submit = { OrderOperationResponse(operation = op) }, waitForSettlement = { op })

        // The fill's venue orderId is a real oid (not the operation id); only
        // its cloid ties it to this handle.
        val matchingFill = makeFill(orderId = "venue-oid-999", cloid = cloid, size = "0.01", price = "72000")
        val deps = unexpectedDeps(
            fillEvents = { flowOf(matchingFill to RealmEvent(realmId = "rlm_test", type = "fill.recorded")) },
        )
        val handle = orderHandle(inner, "/op/order/bracket-1", deps)

        var receivedFill: SimFill? = null
        val latch = CountDownLatch(1)
        val unsub = handle.onFill { fill ->
            receivedFill = fill
            latch.countDown()
        }
        assertTrue(latch.await(2, TimeUnit.SECONDS))
        assertEquals(cloid, receivedFill?.cloid)
        unsub()
    }

    @Test
    fun cancelGeneratesCorrectPath() = runBlocking {
        val op = makeOrderOperation(state = OperationState.COMPLETED, outcome = "ord_abc")
        val inner = OperationHandle(scope, submit = { OrderOperationResponse(operation = op) }, waitForSettlement = { op })

        var capturedCancelPath: String? = null
        var capturedObjectId: String? = null
        var capturedOrderId: String? = null
        val cancelOp = makeOrderOperation(id = "op_cancel_1", state = OperationState.COMPLETED)

        val deps = unexpectedDeps(
            cancelOrder = { path, objId, ordId ->
                capturedCancelPath = path
                capturedObjectId = objId
                capturedOrderId = ordId
                OperationHandle(scope, submit = { OrderOperationResponse(operation = cancelOp) }, waitForSettlement = { cancelOp })
            },
            waitForSettlement = { cancelOp },
        )
        val handle = orderHandle(inner, "/op/order/btc-buy-1", deps)

        val result = handle.cancel().settled()
        assertEquals("/op/order/btc-buy-1/cancel", capturedCancelPath)
        assertEquals("obj_exchange", capturedObjectId)
        assertEquals("ord_abc", capturedOrderId)
        assertEquals(OperationState.COMPLETED, result.operation.state)
    }

    @Test
    fun cancelWithCustomPath() = runBlocking {
        val op = makeOrderOperation(state = OperationState.COMPLETED, outcome = "ord_abc")
        val inner = OperationHandle(scope, submit = { OrderOperationResponse(operation = op) }, waitForSettlement = { op })

        var capturedCancelPath: String? = null
        val cancelOp = makeOrderOperation(id = "op_cancel_2", state = OperationState.COMPLETED)
        val deps = unexpectedDeps(
            cancelOrder = { path, _, _ ->
                capturedCancelPath = path
                OperationHandle(scope, submit = { OrderOperationResponse(operation = cancelOp) }, waitForSettlement = { cancelOp })
            },
            waitForSettlement = { cancelOp },
        )
        val handle = orderHandle(inner, "/op/order/btc-buy-1", deps)

        handle.cancel(path = "/op/order/custom-cancel").settled()
        assertEquals("/op/order/custom-cancel", capturedCancelPath)
    }

    @Test
    fun resizeForwardsNewSizeAndAutoPath() = runBlocking {
        val op = makeOrderOperation(state = OperationState.COMPLETED, outcome = "ord_abc")
        val inner = OperationHandle(scope, submit = { OrderOperationResponse(operation = op) }, waitForSettlement = { op })

        var capturedPath: String? = null
        var capturedObjectId: String? = null
        var capturedOrderId: String? = null
        var capturedNewSize: String? = null
        val modifyOp = makeOrderOperation(id = "op_modify_1", state = OperationState.COMPLETED)

        val deps = unexpectedDeps(
            modifyOrder = { path, objId, ordId, newSize ->
                capturedPath = path
                capturedObjectId = objId
                capturedOrderId = ordId
                capturedNewSize = newSize
                OperationHandle(scope, submit = { OrderOperationResponse(operation = modifyOp) }, waitForSettlement = { modifyOp })
            },
            waitForSettlement = { modifyOp },
        )
        val handle = orderHandle(inner, "/op/order/btc-buy-1", deps)

        val result = handle.resize("0.75").settled()
        assertEquals("/op/modify/btc-buy-1-0.75", capturedPath)
        assertEquals("obj_exchange", capturedObjectId)
        assertEquals("ord_abc", capturedOrderId)
        assertEquals("0.75", capturedNewSize)
        assertEquals(OperationState.COMPLETED, result.operation.state)
    }

    @Test
    fun resizeWithCustomPath() = runBlocking {
        val op = makeOrderOperation(state = OperationState.COMPLETED, outcome = "ord_abc")
        val inner = OperationHandle(scope, submit = { OrderOperationResponse(operation = op) }, waitForSettlement = { op })

        var capturedPath: String? = null
        val modifyOp = makeOrderOperation(id = "op_modify_2", state = OperationState.COMPLETED)
        val deps = unexpectedDeps(
            modifyOrder = { path, _, _, _ ->
                capturedPath = path
                OperationHandle(scope, submit = { OrderOperationResponse(operation = modifyOp) }, waitForSettlement = { modifyOp })
            },
            waitForSettlement = { modifyOp },
        )
        val handle = orderHandle(inner, "/op/order/btc-buy-1", deps)

        handle.resize("2", path = "/op/modify/custom").settled()
        assertEquals("/op/modify/custom", capturedPath)
    }

    @Test
    fun filledReturnsOnIOCPartialFill() = runBlocking {
        val op = makeOrderOperation(state = OperationState.COMPLETED, outcome = "ord_abc")
        val inner = OperationHandle(scope, submit = { OrderOperationResponse(operation = op) }, waitForSettlement = { op })

        val partialOrder = makeSimOrder(status = OrderStatus.CANCELLED, size = "1.372", filledSize = "1.1932", timeInForce = TimeInForce.IOC)
        val orderWithFills = SimOrderWithFills(
            order = partialOrder,
            fills = listOf(makeFill(orderId = "ord_abc", size = "1.1932", price = "2000")),
        )
        val deps = unexpectedDeps(
            getOrder = { _, _ -> orderWithFills },
            fillEvents = { emptyFlow() },
        )
        val handle = orderHandle(inner, "/op/order/eth-sell-1", deps)

        val result = handle.filled(timeoutSeconds = 2.0)
        assertEquals(OrderStatus.CANCELLED, result.order.status)
        assertEquals("1.1932", result.order.filledSize)
        assertTrue(result.order.isPartiallyFilled)
        assertTrue(result.order.isTerminalWithFills)
    }

    @Test
    fun filledThrowsOnCancelledWithNoFills() = runBlocking {
        val op = makeOrderOperation(state = OperationState.COMPLETED, outcome = "ord_abc")
        val inner = OperationHandle(scope, submit = { OrderOperationResponse(operation = op) }, waitForSettlement = { op })

        val cancelledOrder = makeSimOrder(status = OrderStatus.CANCELLED, size = "1.0", filledSize = "0")
        val orderWithFills = SimOrderWithFills(order = cancelledOrder, fills = emptyList())
        val deps = unexpectedDeps(
            getOrder = { _, _ -> orderWithFills },
            fillEvents = { emptyFlow() },
        )
        val handle = orderHandle(inner, "/op/order/eth-sell-2", deps)

        val thrown = runCatching { handle.filled(timeoutSeconds = 2.0) }.exceptionOrNull()
        val unknown = thrown as? ArcaException.Unknown
        assertTrue(unknown != null, "Expected ArcaException.Unknown, got $thrown")
        assertEquals("ORDER_CANCELLED", unknown?.code)
    }

    @Test
    fun simOrderIsPartiallyFilled() {
        val partial = makeSimOrder(status = OrderStatus.CANCELLED, size = "1.372", filledSize = "1.1932")
        assertTrue(partial.isPartiallyFilled)
        assertTrue(partial.isTerminalWithFills)

        val full = makeSimOrder(status = OrderStatus.FILLED, size = "1.0", filledSize = "1.0")
        assertFalse(full.isPartiallyFilled)
        assertTrue(full.isTerminalWithFills)

        val noFill = makeSimOrder(status = OrderStatus.CANCELLED, size = "1.0", filledSize = "0")
        assertFalse(noFill.isPartiallyFilled)
        assertFalse(noFill.isTerminalWithFills)

        val open = makeSimOrder(status = OrderStatus.OPEN, size = "1.0", filledSize = "0")
        assertFalse(open.isPartiallyFilled)
        assertFalse(open.isTerminalWithFills)
    }
}
