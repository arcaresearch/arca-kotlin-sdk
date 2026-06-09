package network.arca.sdk

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
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
): Operation = Operation(
    id = OperationId(id),
    realmId = RealmId("rlm_test"),
    path = "/op/order/btc-buy-1",
    type = OperationType.ORDER,
    state = state,
    outcome = outcome,
    actorType = "user",
    createdAt = "2026-03-08T00:00:00.000000Z",
    updatedAt = "2026-03-08T00:00:00.000000Z",
)

private fun makeFill(
    id: String = "fill_1",
    orderId: String = "ord_abc",
    size: String = "0.5",
    price: String = "50000",
): SimFill = SimFill(
    id = SimFillId(id),
    orderId = SimOrderId(orderId),
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
    ): OrderHandleDeps = OrderHandleDeps(getOrder, fillEvents, cancelOrder, modifyOrder, waitForSettlement, listFills)

    private fun orderHandle(inner: OperationHandle<OrderOperationResponse>, placementPath: String, deps: OrderHandleDeps): OrderHandle =
        OrderHandle(scope = scope, inner = inner, objectId = "obj_exchange", placementPath = placementPath, deps = deps)

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
