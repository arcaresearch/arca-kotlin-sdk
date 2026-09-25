package network.arca.sdk

import kotlinx.coroutines.*
import kotlinx.serialization.json.*
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.ConnectionStatus
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.mockwebserver.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * Operation waits used to hold a realm-root watch: every wait assembled a
 * full-realm snapshot (a realm-wide operations scan) and put every realm
 * event on the socket. They now subscribe to operation events by type and
 * use the server's acknowledgement of that subscription as the barrier
 * before their read.
 */
class OperationWatchRecoveryTest {
    private fun operation(state: String = "pending", id: String = "op") = """{"id":"$id","realmId":"realm","path":"/op","type":"order","state":"$state","createdAt":"2026-01-01","updatedAt":"2026-01-01"}"""
    private fun ack(requestId: String, types: String = """["operation.created","operation.updated"]""") =
        """{"type":"events_subscribed","requestId":"$requestId","types":$types}"""
    private inner class Harness : AutoCloseable {
        val server = MockWebServer()
        val reads = AtomicInteger()
        val failures = AtomicInteger()
        val sent = CopyOnWriteArrayList<JsonObject>()
        @Volatile var socket: WebSocket? = null
        @Volatile var state = "pending"
        @Volatile var acknowledge = true
        @Volatile var lastAck: String? = null
        @Volatile var lastWatchRequestId: String? = null
        val arca: Arca
        init {
            server.dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    if (request.getHeader("Upgrade")?.equals("websocket", true) == true) return MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                        override fun onOpen(ws: WebSocket, response: okhttp3.Response) { socket = ws }
                        override fun onMessage(ws: WebSocket, text: String) {
                            val json = arcaJson.parseToJsonElement(text).jsonObject
                            sent += json
                            when (json["action"]?.jsonPrimitive?.content) {
                                "auth" -> ws.send("""{"type":"authenticated"}""")
                                "watch" -> lastWatchRequestId = json["requestId"]?.jsonPrimitive?.contentOrNull
                                "subscribe_events" -> {
                                    val requestId = json["requestId"]?.jsonPrimitive?.contentOrNull ?: return
                                    val reply = ack(requestId, json["types"].toString())
                                    lastAck = reply
                                    if (acknowledge) ws.send(reply)
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
        fun actions() = sent.mapNotNull { it["action"]?.jsonPrimitive?.contentOrNull }
        override fun close() { arca.close(); server.shutdown() }
    }
    private suspend fun waitFor(condition: () -> Boolean) = withTimeout(2500) { while (!condition()) delay(5) }

    @Test fun subscribesToOperationEventsByTypeAndNeverWatchesTheRoot() = runBlocking<Unit> {
        Harness().use { h ->
            h.state = "completed"
            assertEquals("op", h.arca.waitForOperation("op", 3.0).id.value)
            assertFalse("watch" in h.actions(), "an operation wait must not watch the realm root")
            val confirmed = h.sent.last { it["action"]?.jsonPrimitive?.contentOrNull == "subscribe_events" && it["requestId"] != null }
            assertEquals(setOf("operation.created", "operation.updated"), confirmed["types"]!!.jsonArray.map { it.jsonPrimitive.content }.toSet())
            waitFor { "unsubscribe_events" in h.actions() }
        }
    }
    @Test fun staleAcknowledgementCannotReleaseTheRead() = runBlocking<Unit> {
        Harness().use { h ->
            h.acknowledge = false
            val waiting = async { h.arca.waitForOperation("op", 3.0) }
            waitFor { h.lastAck != null }; delay(30)
            h.socket!!.send(ack("stale-request")); delay(50)
            assertEquals(0, h.reads.get()); assertFalse(waiting.isCompleted)
            h.socket!!.send(h.lastAck!!)
            waitFor { h.reads.get() == 1 }; assertFalse(waiting.isCompleted)
            h.socket!!.send("""{"type":"operation.updated","entityId":"op","operation":${operation("completed")}}""")
            assertEquals("op", waiting.await().id.value); assertEquals(1, h.reads.get())
        }
    }
    @Test fun rotationRequiresAFreshReplacementAcknowledgement() = runBlocking<Unit> {
        for (state in listOf("completed", "failed", "expired")) Harness().use { h ->
            h.acknowledge = false
            val waiting = async { runCatching { h.arca.waitForOperation("op", 4.0) } }
            waitFor { h.lastAck != null }; delay(30)
            val original = h.socket!!
            original.send(h.lastAck!!)
            waitFor { h.reads.get() == 1 }
            h.state = state
            val initial = h.lastAck
            assertTrue(h.arca.ws.rotateConnection())
            waitFor { h.socket !== original && h.lastAck != initial }; delay(30)
            val replacement = h.socket!!
            val warming = h.lastAck!!
            replacement.send(warming); delay(30)
            assertEquals(1, h.reads.get(), "Warming traffic cannot release a read")
            replacement.send("""{"type":"pong"}""")
            waitFor { h.lastAck != warming }; delay(30)
            replacement.send(warming); delay(30)
            assertEquals(1, h.reads.get(), "A stale acknowledgement is not a fresh barrier")
            replacement.send(h.lastAck!!)
            try {
                val result = waiting.await().getOrThrow()
                assertEquals("completed", state); assertEquals("op", result.id.value)
            } catch (e: ArcaException.OperationFailed) {
                assertNotEquals("completed", state); assertEquals(state, e.operation.state.wire)
            }
            assertEquals(2, h.reads.get())
        }
    }
    /** Another owner's realm-root watch snapshot still carries terminal evidence the wait accepts without a read. */
    @Test fun anotherRootOwnersSnapshotCompletesTheWait() = runBlocking<Unit> {
        Harness().use { h ->
            h.acknowledge = false
            h.arca.ws.watchPath("/")
            waitFor { h.arca.ws.status == ConnectionStatus.CONNECTED && h.lastWatchRequestId != null }; delay(50)
            for (state in listOf("completed", "failed", "expired")) {
                val waiting = async { runCatching { h.arca.waitForOperation("op", 3.0) } }
                delay(30)
                h.socket!!.send("""{"type":"watch_snapshot","path":"/","requestId":"${h.lastWatchRequestId}","operations":[${operation("completed", "foreign")},${operation()}],"bufferedOperations":[${operation(state)}]}""")
                try {
                    val result = waiting.await().getOrThrow()
                    assertEquals("completed", state); assertEquals("op", result.id.value)
                } catch (e: ArcaException.OperationFailed) {
                    assertNotEquals("completed", state); assertEquals(state, e.operation.state.wire)
                }
                assertEquals(0, h.reads.get(), "Shared terminal evidence must not GET")
            }
            h.arca.ws.unwatchPath("/")
        }
    }
    @Test fun freshAckPrecedesReadAndHealthyPendingOnlyRecoversAfterActualGap() = runBlocking<Unit> {
        Harness().use { h ->
            h.acknowledge = false
            val waiting = async { h.arca.waitForOperation("op", 6.0) }
            waitFor { h.lastAck != null }; delay(30)
            assertEquals(0, h.reads.get(), "the read must follow the acknowledged subscription")
            h.acknowledge = true; h.socket!!.send(h.lastAck!!)
            waitFor { h.reads.get() == 1 }; delay(2100)
            assertEquals(1, h.reads.get(), "healthy pending operation must not poll")
            h.state = "completed"; h.socket!!.send("""{"type":"stream.resync"}""")
            assertEquals("op", waiting.await().id.value); assertEquals(2, h.reads.get())
        }
    }
    @Test fun failedReadsHaveFiniteBudgetAndLiveTerminalStillCompletes() = runBlocking<Unit> {
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
