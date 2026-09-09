package network.arca.sdk

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.emptyFlow
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.*
import kotlinx.serialization.encodeToString
import okhttp3.OkHttpClient
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class OrderCaptureLifecycleTest {
    private fun operation(outcome: String? = null, account: String = "account") = Operation(
        id = OperationId("op"), realmId = RealmId("realm"), path = "/op", type = OperationType.ORDER, state = OperationState.PENDING,
        input = """{"exchangeObjectId":"$account","size":"3","timeInForce":"IOC"}""", outcome = outcome,
        createdAt = "2026-01-01", updatedAt = "2026-01-01")
    private fun operationEvent(operation: Operation) = """{"type":"operation.updated","operation":${arcaJson.encodeToString(operation)}}"""
    private fun terminal(order: String = "venue", account: String = "account") = """{"type":"order.updated","entityId":"$account","order":{"order":{"id":"$order","status":"FILLED","filledSize":"1"}}}"""
    private suspend fun waitFor(condition: () -> Boolean) = withTimeout(2000) { while (!condition()) delay(5) }
    private inner class Harness : AutoCloseable {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val factory = FakeSocketFactory()
        val ws = WebSocketManager(baseUrl = "http://localhost:19999", token = "test", realmId = "realm", httpClient = OkHttpClient(), socketFactory = factory, connectionLifetimeMs = 0)
        lateinit var capture: OrderEventCapture
        val socket get() = factory[0]
        suspend fun start() {
            ws.connect(); waitFor { factory.count > 0 }; socket.deliver("""{"type":"authenticated"}""")
            waitFor { ws.status == ConnectionStatus.CONNECTED }
            capture = OrderEventCapture(scope, ws)
        }
        override fun close() { scope.cancel(); ws.shutdown() }
    }
    @Test fun submittedOnlyLearnsIdentityAndReleasesOnTerminalInEitherOrder() = runBlocking<Unit> {
        for (terminalFirst in listOf(false, true)) Harness().use { h ->
            h.start(); h.capture.submitted(operation(), "account")
            if (terminalFirst) h.ws.injectMessage(terminal())
            h.ws.injectMessage(operationEvent(operation("""{"orderId":"venue","status":"OPEN","filledSize":"0"}""")))
            if (!terminalFirst) h.ws.injectMessage(terminal())
            waitFor { "unwatch" in h.socket.actions() }
        }
    }
    @Test fun bufferedBeforeSubmissionAndWrongAccountOrderDoNotLeakOrReleaseEarly() = runBlocking<Unit> {
        Harness().use { h ->
            h.start(); h.ws.injectMessage(terminal())
            h.ws.injectMessage(operationEvent(operation("""{"orderId":"wrong"}""", "foreign")))
            h.capture.submitted(operation(), "account")
            delay(30); assertFalse("unwatch" in h.socket.actions())
            h.ws.injectMessage(terminal(order = "other")); h.ws.injectMessage(terminal(account = "foreign"))
            delay(30); assertFalse("unwatch" in h.socket.actions())
            h.ws.injectMessage(operationEvent(operation("""{"orderId":"venue","status":"OPEN","filledSize":"0"}""")))
            waitFor { "unwatch" in h.socket.actions() }
        }
    }
    @Test fun knownOrderCannotBeReplacedByAnotherLegAndKeepsOtherWatchOwner() = runBlocking<Unit> {
        Harness().use { h ->
            h.start(); h.ws.watchPath("/")
            h.capture.submitted(operation("""{"orderId":"venue","status":"OPEN","filledSize":"0"}"""), "account")
            h.ws.injectMessage(operationEvent(operation("""{"orderId":"other","status":"OPEN","filledSize":"0"}""")))
            h.ws.injectMessage(terminal(order = "other")); delay(30)
            h.ws.unwatchPath("/"); assertFalse("unwatch" in h.socket.actions())
            h.ws.injectMessage(terminal()); waitFor { "unwatch" in h.socket.actions() }
        }
    }
    @Test fun receiptTimeoutStillReleasesLaterWithoutRetry() = runBlocking<Unit> {
        Harness().use { h ->
            h.start(); val original = operation(); h.capture.submitted(original, "account")
            val inner = OperationHandle(h.scope, submit = { OrderOperationResponse(original) }, waitForSettlement = { original })
            val deps = OrderHandleDeps(getOrder = { _, _ -> error("unexpected") }, fillEvents = { emptyFlow() },
                cancelOrder = { _, _, _ -> error("unexpected") }, modifyOrder = { _, _, _, _ -> error("unexpected") },
                waitForSettlement = { original }, listFills = { FillListResponse(emptyList(), 0) },
                releaseExecution = { h.capture.stop() }, awaitExecutionReady = {}, executionEvents = { h.capture.events }, getExecutionOperation = { original })
            val handle = OrderHandle(scope = h.scope, inner = inner, objectId = "account", placementPath = "/op", deps = deps)
            val error = runCatching { handle.executionReceipt(timeoutSeconds = 0.02) }.exceptionOrNull()
            assertTrue(error is ArcaException.Unknown && error.code == "TIMEOUT"); assertFalse("unwatch" in h.socket.actions())
            h.ws.injectMessage(operationEvent(operation("""{"orderId":"venue","status":"OPEN","filledSize":"0"}""")))
            h.ws.injectMessage(terminal()); waitFor { "unwatch" in h.socket.actions() }
            handle.submitted()
        }
    }
}
