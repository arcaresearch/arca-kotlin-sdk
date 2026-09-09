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
                                    val ack = """{"type":"watch_snapshot","path":${json["path"]},"requestId":${json["requestId"]}}"""
                                    lastAck = ack
                                    if (acknowledge) ws.send(ack)
                                }
                            }
                        }
                    })
                    reads.incrementAndGet()
                    if (failures.getAndUpdate { maxOf(0, it - 1) } > 0) return MockResponse().setResponseCode(422).setHeader("Content-Type", "application/json").setBody("""{"success":false,"error":{"code":"VALIDATION","message":"fixture failure"}}""")
                    return MockResponse().setHeader("Content-Type", "application/json").setBody("""{"success":true,"data":{"operation":${operation(state)}}}""")
                }
            }
            server.start()
            arca = Arca(token = "test", realmId = "realm", baseUrl = server.url("/").toString().trimEnd('/'))
        }
        override fun close() { arca.close(); server.shutdown() }
    }
    private suspend fun waitFor(condition: () -> Boolean) = withTimeout(2500) { while (!condition()) delay(5) }
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
