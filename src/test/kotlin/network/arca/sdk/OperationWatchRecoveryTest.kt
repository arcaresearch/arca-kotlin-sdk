package network.arca.sdk

import kotlinx.coroutines.*
import kotlinx.serialization.json.*
import network.arca.sdk.internal.arcaJson
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.mockwebserver.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger

class OperationWatchRecoveryTest {
    private fun operation(state: String = "pending", id: String = "op") = """{"id":"$id","realmId":"realm","path":"/op","type":"order","state":"$state","createdAt":"2026-01-01","updatedAt":"2026-01-01"}"""
    private inner class Harness : AutoCloseable {
        val server = MockWebServer()
        val reads = AtomicInteger()
        val failures = AtomicInteger()
        @Volatile var socket: WebSocket? = null
        @Volatile var state = "pending"
        @Volatile var acknowledge = true
        @Volatile var lastAck: String? = null
        @Volatile var snapshotOperations = "[]"
        @Volatile var bufferedOperations = "[]"
        val arca: Arca
        init {
            server.dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    if (request.getHeader("Upgrade")?.equals("websocket", true) == true) return MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                        override fun onOpen(ws: WebSocket, response: okhttp3.Response) { socket = ws }
                        override fun onMessage(ws: WebSocket, text: String) {
                            val json = arcaJson.parseToJsonElement(text).jsonObject
                            when (json["action"]?.jsonPrimitive?.content) {
                                "auth" -> ws.send("""{"type":"authenticated"}""")
                                "watch" -> {
                                    val ack = """{"type":"watch_snapshot","path":${json["path"]},"requestId":${json["requestId"]},"operations":$snapshotOperations,"bufferedOperations":$bufferedOperations}"""
                                    lastAck = ack
                                    if (acknowledge) ws.send(ack)
                                }
                            }
                        }
                    })
                    reads.incrementAndGet()
                    if (failures.getAndUpdate { maxOf(0, it - 1) } > 0) return MockResponse().setResponseCode(422).setHeader("Content-Type", "application/json").setBody("""{"success":false,"error":{"code":"VALIDATION","message":"fixture failure"}}""")
                    return MockResponse().setHeader("Content-Type", "application/json").setBody("""{"success":true,"data":{"operation":${operation(state, request.requestUrl!!.pathSegments.last())}}}""")
                }
            }
            server.start()
            arca = Arca(token = "test", realmId = "realm", baseUrl = server.url("/").toString().trimEnd('/'))
        }
        override fun close() { arca.close(); server.shutdown() }
    }
    private suspend fun waitFor(condition: () -> Boolean) = withTimeout(2500) { while (!condition()) delay(5) }
    @Test fun terminalSnapshotAndBufferedTransitionsCompleteWithoutHttp() = runBlocking<Unit> {
        Harness().use { h ->
            h.failures.set(99)
            for (state in listOf("completed", "failed", "expired")) for (buffered in listOf(false, true)) {
                h.snapshotOperations = "[${operation("completed", "foreign")},${operation(if (buffered) "pending" else state)}]"
                h.bufferedOperations = if (buffered) "[${operation(state)}]" else "[]"
                try {
                    val result = h.arca.waitForOperation("op", 3.0)
                    assertEquals("completed", state); assertEquals("op", result.id.value)
                } catch (e: ArcaException.OperationFailed) {
                    assertNotEquals("completed", state); assertEquals(state, e.operation.state.wire)
                }
                assertEquals(0, h.reads.get(), "Terminal snapshot must precede HTTP recovery")
            }
        }
    }
    @Test fun staleSnapshotAndForeignTerminalCannotCompleteWait() = runBlocking<Unit> {
        Harness().use { h ->
            h.acknowledge = false
            val waiting = async { h.arca.waitForOperation("op", 3.0) }
            waitFor { h.lastAck != null }; delay(30)
            val ack = arcaJson.parseToJsonElement(h.lastAck!!).jsonObject
            val stale = JsonObject(ack + mapOf("requestId" to JsonPrimitive("stale-request"),
                "operations" to arcaJson.parseToJsonElement("[${operation("completed")}]")))
            h.socket!!.send(stale.toString()); delay(50)
            assertEquals(0, h.reads.get()); assertFalse(waiting.isCompleted)
            val current = JsonObject(ack + mapOf("operations" to arcaJson.parseToJsonElement("[${operation()}]"),
                "bufferedOperations" to arcaJson.parseToJsonElement("[${operation("failed", "foreign")}]")))
            h.socket!!.send(current.toString())
            waitFor { h.reads.get() == 1 }; assertFalse(waiting.isCompleted)
            h.socket!!.send("""{"type":"operation.updated","entityId":"op","operation":${operation("completed")}}""")
            assertEquals("op", waiting.await().id.value); assertEquals(1, h.reads.get())
        }
    }
    @Test fun rotationRequiresFreshReplacementSnapshotForTerminalEvidence() = runBlocking<Unit> {
        Harness().use { h ->
            h.acknowledge = false
            fun snapshot(ack: String, state: String): String {
                val obj = arcaJson.parseToJsonElement(ack).jsonObject
                return JsonObject(obj + mapOf("operations" to arcaJson.parseToJsonElement("[${operation()}]"),
                    "bufferedOperations" to arcaJson.parseToJsonElement("[${operation(state)}]"))).toString()
            }
            for (healthy in listOf(false, true)) for (state in listOf("completed", "failed", "expired")) {
                val before = h.reads.get()
                val oldAck = h.lastAck
                val waiting = async { runCatching { h.arca.waitForOperation("op", 4.0) } }
                waitFor { h.lastAck != null && h.lastAck != oldAck }; delay(30)
                val original = h.socket!!
                if (healthy) {
                    original.send(snapshot(h.lastAck!!, "pending"))
                    waitFor { h.reads.get() == before + 1 }
                }
                val initial = h.lastAck
                assertTrue(h.arca.ws.rotateConnection())
                waitFor { h.socket !== original && h.lastAck != initial }; delay(30)
                val replacement = h.socket!!
                val warming = h.lastAck!!
                replacement.send(snapshot(warming, state)); delay(20)
                assertFalse(waiting.isCompleted, "Warming traffic cannot complete a live wait")
                replacement.send("""{"type":"pong"}""")
                waitFor { h.lastAck != warming }; delay(30)
                replacement.send(snapshot(warming, state)); delay(20)
                assertFalse(waiting.isCompleted, "Pong and stale snapshots carry no fresh payload")
                assertEquals(before + if (healthy) 1 else 0, h.reads.get())
                replacement.send(snapshot(h.lastAck!!, state))
                try {
                    val result = waiting.await().getOrThrow()
                    assertEquals("completed", state); assertEquals("op", result.id.value)
                } catch (e: ArcaException.OperationFailed) {
                    assertNotEquals("completed", state); assertEquals(state, e.operation.state.wire)
                }
                assertEquals(before + if (healthy) 1 else 0, h.reads.get(), "Terminal replacement payload must not GET")
            }
        }
    }
    @Test fun sharedSnapshotCompletesOtherWaiterAndPreservesOwner() = runBlocking<Unit> {
        Harness().use { h ->
            for (state in listOf("completed", "failed", "expired")) {
                val before = h.reads.get()
                h.acknowledge = true
                val a = async { runCatching { h.arca.waitForOperation("op", 3.0) } }
                waitFor { h.reads.get() == before + 1 }
                h.acknowledge = false
                val old = h.lastAck
                val b = async { h.arca.waitForOperation("op-b", 3.0) }
                waitFor { h.lastAck != old }; delay(30)
                val ack = arcaJson.parseToJsonElement(h.lastAck!!).jsonObject
                h.socket!!.send(JsonObject(ack + mapOf("operations" to arcaJson.parseToJsonElement("[${operation("pending", "op-b")}]"),
                    "bufferedOperations" to arcaJson.parseToJsonElement("[${operation(state)}]"))).toString())
                try {
                    val result = a.await().getOrThrow()
                    assertEquals("completed", state); assertEquals("op", result.id.value)
                } catch (e: ArcaException.OperationFailed) {
                    assertNotEquals("completed", state); assertEquals(state, e.operation.state.wire)
                }
                waitFor { h.reads.get() == before + 2 }; assertFalse(b.isCompleted)
                h.socket!!.send("""{"type":"operation.updated","entityId":"op-b","operation":${operation("completed", "op-b")}}""")
                assertEquals("op-b", b.await().id.value); assertEquals(before + 2, h.reads.get())
            }
        }
    }
    @Test fun freshAckPrecedesSnapshotAndHealthyPendingOnlyRecoversAfterActualGap() = runBlocking<Unit> {
        Harness().use { h ->
            h.acknowledge = false
            val waiting = async { h.arca.waitForOperation("op", 6.0) }
            waitFor { h.lastAck != null }; delay(30)
            assertEquals(0, h.reads.get(), "bootstrap read must follow correlated snapshot ACK")
            h.acknowledge = true; h.socket!!.send(h.lastAck!!)
            waitFor { h.reads.get() == 1 }; delay(2100)
            assertEquals(1, h.reads.get(), "healthy pending operation must not poll")
            h.state = "completed"; h.socket!!.send("""{"type":"stream.resync"}""")
            assertEquals("op", waiting.await().id.value); assertEquals(2, h.reads.get())
        }
    }
    @Test fun failedSnapshotsHaveFiniteBudgetAndLiveTerminalStillCompletes() = runBlocking<Unit> {
        Harness().use { h ->
            h.failures.set(9)
            val waiting = async { h.arca.waitForOperation("op", 6.0) }
            waitFor { h.reads.get() == 3 }; delay(2100)
            assertEquals(3, h.reads.get())
            h.socket!!.send("""{"type":"operation.updated","entityId":"op","operation":${operation("completed")}}""")
            assertEquals("op", waiting.await().id.value); assertEquals(3, h.reads.get())
        }
    }
    @Test fun terminalPushBeatsMissingAcknowledgement() = runBlocking<Unit> {
        Harness().use { h ->
            h.acknowledge = false
            val waiting = async { h.arca.waitForOperation("op", 3.0) }
            waitFor { h.lastAck != null }
            h.socket!!.send("""{"type":"operation.updated","entityId":"op","operation":${operation("completed")}}""")
            assertEquals("op", withTimeout(500) { waiting.await() }.id.value)
            assertEquals(0, h.reads.get())
        }
    }
    @Test fun sparseOperationNotificationRecoversButForeignPayloadDoesNot() = runBlocking<Unit> {
        Harness().use { h ->
            val waiting = async { h.arca.waitForOperation("op", 3.0) }
            waitFor { h.reads.get() == 1 }
            h.socket!!.send("""{"type":"operation.updated","entityId":"op","operation":${operation("completed", "foreign")}}""")
            delay(100); assertEquals(1, h.reads.get()); assertFalse(waiting.isCompleted)
            h.state = "completed"; h.socket!!.send("""{"type":"operation.updated","entityId":"op"}""")
            assertEquals("op", waiting.await().id.value); assertEquals(2, h.reads.get())
        }
    }
}
