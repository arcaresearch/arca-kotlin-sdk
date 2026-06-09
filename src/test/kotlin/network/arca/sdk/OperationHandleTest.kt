package network.arca.sdk

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import network.arca.sdk.models.ArcaObject
import network.arca.sdk.models.ArcaObjectStatus
import network.arca.sdk.models.ArcaObjectType
import network.arca.sdk.models.CreateArcaObjectResponse
import network.arca.sdk.models.DefundAccountResponse
import network.arca.sdk.models.DeleteArcaObjectResponse
import network.arca.sdk.models.FundAccountResponse
import network.arca.sdk.models.Operation
import network.arca.sdk.models.OperationState
import network.arca.sdk.models.OperationType
import network.arca.sdk.models.OrderOperationResponse
import network.arca.sdk.models.TransferResponse
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private fun makeOperation(id: String = "op_123", state: OperationState = OperationState.COMPLETED): Operation =
    Operation(
        id = OperationId(id),
        realmId = RealmId("rlm_test"),
        path = "/op/test/1",
        type = OperationType.TRANSFER,
        state = state,
        sourceArcaPath = "/wallets/a",
        targetArcaPath = "/wallets/b",
        actorType = "user",
        createdAt = "2026-03-08T00:00:00.000000Z",
        updatedAt = "2026-03-08T00:00:00.000000Z",
    )

private data class TestResponse(
    override val operation: Operation,
    val value: String,
) : OperationResponse {
    override fun withOperation(operation: Operation): TestResponse = copy(operation = operation)
}

class OperationHandleTest {

    private lateinit var scope: CoroutineScope

    @BeforeEach
    fun setUp() {
        scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    }

    @AfterEach
    fun tearDown() {
        scope.cancel()
    }

    @Test
    fun settledResolvesImmediatelyForNonPendingOperation() = runBlocking {
        val op = makeOperation(state = OperationState.COMPLETED)
        val response = TestResponse(op, "hello")
        val handle = OperationHandle(
            scope,
            submit = { response },
            waitForSettlement = {
                throw AssertionError("waitForSettlement should not be called for non-pending operations")
            },
        )
        val result = handle.settled()
        assertEquals("hello", result.value)
        assertEquals(OperationState.COMPLETED, result.operation.state)
    }

    @Test
    fun settledWaitsForPendingOperation() = runBlocking {
        val pendingOp = makeOperation(state = OperationState.PENDING)
        val completedOp = makeOperation(state = OperationState.COMPLETED)
        val response = TestResponse(pendingOp, "world")
        val handle = OperationHandle(
            scope,
            submit = { response },
            waitForSettlement = { operationId ->
                assertEquals("op_123", operationId)
                completedOp
            },
        )
        val result = handle.settled()
        assertEquals("world", result.value)
        assertEquals(OperationState.COMPLETED, result.operation.state)
    }

    @Test
    fun submittedResolvesBeforeSettlement() = runBlocking {
        val pendingOp = makeOperation(state = OperationState.PENDING)
        val response = TestResponse(pendingOp, "early")
        val handle = OperationHandle(
            scope,
            submit = { response },
            waitForSettlement = {
                delay(200)
                makeOperation(state = OperationState.COMPLETED)
            },
        )
        val submitted = handle.submitted()
        assertEquals("early", submitted.value)
        assertEquals(OperationState.PENDING, submitted.operation.state)
    }

    @Test
    fun settledPropagatesSubmitError() = runBlocking {
        val handle = OperationHandle<TestResponse>(
            scope,
            submit = { throw ArcaException.Validation("bad input", null) },
            waitForSettlement = {
                throw AssertionError("Should not reach settlement")
            },
        )
        val thrown = runCatching { handle.settled() }.exceptionOrNull()
        val v = thrown as? ArcaException.Validation
        assertTrue(v != null, "Expected validation error, got $thrown")
        assertEquals("bad input", v?.message)
    }

    @Test
    fun settledPropagatesSettlementError() = runBlocking {
        val failedOp = makeOperation(state = OperationState.FAILED)
        val pendingOp = makeOperation(state = OperationState.PENDING)
        val response = TestResponse(pendingOp, "will fail")
        val handle = OperationHandle(
            scope,
            submit = { response },
            waitForSettlement = { throw ArcaException.OperationFailed(failedOp) },
        )
        val thrown = runCatching { handle.settled() }.exceptionOrNull()
        val of = thrown as? ArcaException.OperationFailed
        assertTrue(of != null, "Expected operationFailed, got $thrown")
        assertEquals(OperationState.FAILED, of?.operation?.state)
    }

    @Test
    fun settledWithTimeoutSucceeds() = runBlocking {
        val op = makeOperation(state = OperationState.COMPLETED)
        val response = TestResponse(op, "fast")
        val handle = OperationHandle(scope, submit = { response }, waitForSettlement = { op })
        val result = handle.settled(timeoutSeconds = 5.0)
        assertEquals("fast", result.value)
    }

    @Test
    fun settledWithTimeoutThrows() = runBlocking {
        val pendingOp = makeOperation(state = OperationState.PENDING)
        val response = TestResponse(pendingOp, "slow")
        val handle = OperationHandle(
            scope,
            submit = { response },
            waitForSettlement = {
                delay(10_000)
                makeOperation(state = OperationState.COMPLETED)
            },
        )
        val thrown = runCatching { handle.settled(timeoutSeconds = 0.1) }.exceptionOrNull()
        val unknown = thrown as? ArcaException.Unknown
        assertTrue(unknown != null, "Expected TIMEOUT error, got $thrown")
        assertEquals("TIMEOUT", unknown?.code)
    }

    @Test
    fun multipleSettledCallsReturnSameResult() = runBlocking {
        val op = makeOperation(state = OperationState.PENDING)
        val response = TestResponse(op, "cached")
        val handle = OperationHandle(
            scope,
            submit = { response },
            waitForSettlement = { makeOperation(state = OperationState.COMPLETED) },
        )
        val r1 = handle.settled()
        val r2 = handle.settled()
        assertEquals("cached", r1.value)
        assertEquals("cached", r2.value)
    }

    @Test
    fun asyncBatching() = runBlocking {
        val op1 = makeOperation(id = "op_1", state = OperationState.COMPLETED)
        val op2 = makeOperation(id = "op_2", state = OperationState.COMPLETED)
        val handle1 = OperationHandle(scope, submit = { TestResponse(op1, "first") }, waitForSettlement = { op1 })
        val handle2 = OperationHandle(scope, submit = { TestResponse(op2, "second") }, waitForSettlement = { op2 })

        val r1 = async { handle1.settled() }
        val r2 = async { handle2.settled() }
        assertEquals("first", r1.await().value)
        assertEquals("second", r2.await().value)
    }
}

class OperationResponseConformanceTest {

    @Test
    fun transferResponseConformance() {
        val original = TransferResponse(operation = makeOperation(state = OperationState.PENDING), fee = null)
        val updated = original.withOperation(makeOperation(state = OperationState.COMPLETED))
        assertEquals(OperationState.COMPLETED, updated.operation.state)
    }

    @Test
    fun fundAccountResponseConformance() {
        val original = FundAccountResponse(
            operation = makeOperation(state = OperationState.PENDING),
            poolAddress = "0xabc",
            tokenAddress = "0xdef",
            chain = "ethereum",
            expiresAt = "2026-03-08T01:00:00.000000Z",
        )
        val updated = original.withOperation(makeOperation(state = OperationState.COMPLETED))
        assertEquals(OperationState.COMPLETED, updated.operation.state)
        assertEquals("0xabc", updated.poolAddress)
        assertEquals("0xdef", updated.tokenAddress)
        assertEquals("ethereum", updated.chain)
    }

    @Test
    fun defundAccountResponseConformance() {
        val original = DefundAccountResponse(operation = makeOperation(state = OperationState.PENDING), txHash = "0x123")
        val updated = original.withOperation(makeOperation(state = OperationState.COMPLETED))
        assertEquals(OperationState.COMPLETED, updated.operation.state)
        assertEquals("0x123", updated.txHash)
    }

    @Test
    fun createArcaObjectResponseConformance() {
        val obj = ArcaObject(
            id = ObjectId("obj_1"), realmId = RealmId("rlm_test"), path = "/wallets/main",
            type = ArcaObjectType.DENOMINATED, denomination = "USD", status = ArcaObjectStatus.ACTIVE,
            systemOwned = false, createdAt = "2026-03-08T00:00:00.000000Z", updatedAt = "2026-03-08T00:00:00.000000Z",
        )
        val original = CreateArcaObjectResponse(`object` = obj, operation = makeOperation(state = OperationState.PENDING))
        val updated = original.withOperation(makeOperation(state = OperationState.COMPLETED))
        assertEquals(OperationState.COMPLETED, updated.operation.state)
        assertEquals("/wallets/main", updated.`object`.path)
    }

    @Test
    fun deleteArcaObjectResponseConformance() {
        val obj = ArcaObject(
            id = ObjectId("obj_1"), realmId = RealmId("rlm_test"), path = "/wallets/old",
            type = ArcaObjectType.DENOMINATED, denomination = "USD", status = ArcaObjectStatus.DELETED,
            deletedAt = "2026-03-08T00:00:00.000000Z", systemOwned = false,
            createdAt = "2026-03-08T00:00:00.000000Z", updatedAt = "2026-03-08T00:00:00.000000Z",
        )
        val original = DeleteArcaObjectResponse(`object` = obj, operation = makeOperation(state = OperationState.PENDING))
        val updated = original.withOperation(makeOperation(state = OperationState.COMPLETED))
        assertEquals(OperationState.COMPLETED, updated.operation.state)
    }

    @Test
    fun orderOperationResponseConformance() {
        val original = OrderOperationResponse(operation = makeOperation(state = OperationState.PENDING))
        val updated = original.withOperation(makeOperation(state = OperationState.COMPLETED))
        assertEquals(OperationState.COMPLETED, updated.operation.state)
    }
}
