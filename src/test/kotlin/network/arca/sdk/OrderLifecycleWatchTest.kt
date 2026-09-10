package network.arca.sdk

import kotlinx.coroutines.*
import kotlinx.serialization.json.*
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.ConnectionStatus
import okhttp3.OkHttpClient
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList

class OrderLifecycleWatchTest {
    private val wire = """{"intent":{"realmId":"realm","objectId":"account","operationId":"original","leg":"0","venue":"gll-testnet","venueAccountId":"123","market":"gllt:3","requestedSize":"9007199254740993.123456789","orderType":"MARKET","side":"buy","timeInForce":"GTC","executionTimeInForce":"IOC","isTrigger":false,"isMarketTrigger":false,"sizeToMax":false,"reduceOnly":false},"venueOrderId":"3:order","submission":"accepted","working":false,"execution":"partial","terminal":true,"executedSize":"3.123456789","executionQuantityFinal":true,"requestedSizeKnown":true,"remainingSize":"9007199254740990","remainingDisposition":"canceled","accountedSize":"0","accountingComplete":false,"averagePrice":"2000.000000001","averagePriceFinal":false,"recoveryRequired":false}"""
    private class Harness(timeoutMs: Long = 150) : AutoCloseable {
        val factory = FakeSocketFactory()
        val manager = WebSocketManager("http://order-watch.test", "test", "realm", OkHttpClient(),
            connectionLifetimeMs = 0, socketFactory = factory)
        val watch = manager.watchOrderLifecycle("account", "original", 0, timeoutMs)
        val socket: FakeWebSocket get() = factory[0]
        init { socket.deliver("""{"type":"authenticated"}""") }
        override fun close() { manager.shutdown() }
    }
    private suspend fun waitFor(condition: () -> Boolean) {
        withTimeout(3_000) { while (!condition()) delay(5) }
    }
    private fun requests(socket: FakeWebSocket): List<JsonObject> = socket.sent.map { arcaJson.parseToJsonElement(it).jsonObject }
        .filter { it["action"]?.jsonPrimitive?.content == "watch_order_lifecycle" }
    private fun frame(request: JsonObject, seq: Int, view: String = wire): String = buildJsonObject {
        put("type", "order.lifecycle.updated"); put("watchId", request["watchId"]!!); put("requestId", request["requestId"]!!)
        put("realmId", "realm"); put("objectId", "account"); put("operationId", "original"); put("leg", "0"); put("deliverySeq", seq)
        put("lifecycle", arcaJson.parseToJsonElement(view))
    }.toString()
    private fun error(request: JsonObject): String = buildJsonObject {
        put("type", "error"); put("requestId", request["requestId"]!!); put("message", "stale denial")
    }.toString()

    @Test fun quietSnapshotGapRotationAndStaleErrorsKeepOriginalIdentity(): Unit = runBlocking {
        Harness().use { h ->
            val updates = CopyOnWriteArrayList<OrderLifecycleUpdate>()
            val reader = launch { h.watch.updates.collect { updates += it } }
            try {
                val first = requests(h.socket).last()
                h.socket.deliver(frame(first, 1))
                waitFor { updates.size == 1 }
                assertEquals("9007199254740993.123456789", updates[0].lifecycle!!.intent.requestedSize)
                assertEquals("9007199254740990", updates[0].lifecycle!!.remainingSize)
                assertFalse(updates[0].lifecycle!!.averagePriceFinal)
                delay(350)
                assertEquals(1, requests(h.socket).size, "Healthy snapshots must stay quiet beyond the deadline")
                h.socket.deliver("""{"type":"stream.resync","deliverySeq":4}""")
                waitFor { requests(h.socket).size == 2 }
                val second = requests(h.socket).last()
                assertEquals(first["watchId"], second["watchId"])
                assertNotEquals(first["requestId"], second["requestId"])
                h.socket.deliver(error(first)); h.socket.deliver(frame(first, 5)); h.socket.deliver(frame(second, 6))
                waitFor { updates.size == 2 }
                assertTrue(h.manager.rotateConnection())
                val replacement = h.factory[1]
                replacement.deliver("""{"type":"authenticated"}""")
                assertTrue(replacement.actions().contains("ping"))
                assertTrue(requests(replacement).isEmpty(), "Warming cannot replace current evidence")
                replacement.deliver("""{"type":"pong"}""")
                val third = requests(replacement).last()
                assertEquals("original", third["operationId"]!!.jsonPrimitive.content)
                assertEquals("account", third["objectId"]!!.jsonPrimitive.content)
                assertEquals("0", third["leg"]!!.jsonPrimitive.content)
                replacement.deliver(error(second)); replacement.deliver(frame(third, 1))
                waitFor { updates.size == 3 }
                h.manager.reconnect()
                val reconnected = h.factory[2]
                reconnected.deliver("""{"type":"authenticated"}""")
                reconnected.deliver(error(third)); reconnected.deliver(frame(requests(reconnected).last(), 1))
                waitFor { updates.size == 4 }
                assertTrue(updates.all { !it.unavailable })
                h.watch.stop(); withTimeout(1_000) { reader.join() }
                val count = requests(reconnected).size
                reconnected.deliver("""{"type":"stream.resync","deliverySeq":2}""")
                delay(350)
                assertEquals(count, requests(reconnected).size)
                assertTrue(reconnected.actions().contains("unwatch_order_lifecycle"))
                assertEquals(ConnectionStatus.CONNECTED, h.manager.status)
            } finally { reader.cancel(); h.watch.stop() }
        }
    }

    @Test fun ackAloneTimesOutAndDisconnectFinishesRecovery(): Unit = runBlocking {
        Harness(50).use { h ->
            val updates = CopyOnWriteArrayList<OrderLifecycleUpdate>()
            val reader = launch { h.watch.updates.collect { updates += it } }
            try {
                val first = requests(h.socket).last()
                h.socket.deliver(buildJsonObject {
                    put("type", "order_lifecycle_watch_created"); put("requestId", first["requestId"]!!); put("watchId", first["watchId"]!!)
                }.toString())
                waitFor { updates.firstOrNull()?.reason == "snapshot_timeout" }
                assertTrue(updates[0].recoverable)
                waitFor { requests(h.socket).size == 2 }
                h.socket.deliver(frame(requests(h.socket).last(), 1))
                waitFor { updates.lastOrNull()?.lifecycle != null }
                delay(100)
                assertEquals(2, requests(h.socket).size)
                val active = requests(h.socket).last()
                h.socket.deliver(buildJsonObject {
                    put("type", "order.lifecycle.updated"); put("requestId", active["requestId"]!!); put("watchId", active["watchId"]!!)
                    put("realmId", "realm"); put("objectId", "account"); put("operationId", "original"); put("leg", "0"); put("deliverySeq", 2)
                    put("unavailable", true); put("recoverable", true); put("reason", "source_unavailable")
                }.toString())
                waitFor { updates.lastOrNull()?.reason == "source_unavailable" }
                waitFor { requests(h.socket).size == 3 }
                h.socket.deliver(frame(requests(h.socket).last(), 3))
                waitFor { updates.lastOrNull()?.lifecycle != null }
                h.manager.disconnect()
                withTimeout(1_000) { reader.join() }
                delay(300)
                assertEquals(3, requests(h.socket).size)
            } finally { reader.cancel(); h.watch.stop() }
        }
    }

    @Test fun foreignEvidenceMissingFinalityAndChangedIntentCannotBeAdopted(): Unit = runBlocking {
        for (defect in listOf("foreign", "missing", "changed")) {
            Harness(1_000).use { h ->
                val updates = CopyOnWriteArrayList<OrderLifecycleUpdate>()
                val reader = launch { h.watch.updates.collect { updates += it } }
                try {
                    val request = requests(h.socket).last()
                    if (defect == "changed") { h.socket.deliver(frame(request, 1)); waitFor { updates.size == 1 } }
                    val bad = when (defect) {
                        "foreign" -> wire.replace("\"objectId\":\"account\"", "\"objectId\":\"foreign\"")
                        "missing" -> wire.replace("\"executionQuantityFinal\":true,", "")
                        else -> wire.replace("\"orderType\":\"MARKET\"", "\"orderType\":\"LIMIT\"")
                    }
                    h.socket.deliver(frame(request, if (defect == "changed") 2 else 1, bad))
                    waitFor { updates.lastOrNull()?.unavailable == true }
                    assertFalse(updates.last().recoverable)
                    assertEquals(if (defect == "changed") "original_intent_changed" else "invalid_order_evidence", updates.last().reason)
                    withTimeout(1_000) { reader.join() }
                    assertTrue(h.socket.actions().contains("unwatch_order_lifecycle"))
                } finally { reader.cancel(); h.watch.stop() }
            }
        }
    }
}
