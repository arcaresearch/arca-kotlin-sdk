package network.arca.sdk

import kotlinx.coroutines.*
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.*
import kotlinx.serialization.json.*
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.mockwebserver.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger

class FillWatchRecoveryTest {
    private fun row(id: String) = """{"id":"$id","fillId":"$id","operationId":"fill-$id","orderOperationId":"original","orderId":"order","market":"gll:test:1","size":"1","price":"100","side":"buy"}"""
    private fun event(id: String, account: String = "account") = """{"type":"fill.recorded","entityId":"$id","entityPath":"/$account","fill":${row(id)}}"""
    private inner class Harness : AutoCloseable {
        val server = MockWebServer()
        val reads = AtomicInteger()
        @Volatile var socket: WebSocket? = null
        @Volatile var page: (RecordedRequest) -> String = { """{"fills":[],"total":0}""" }
        val arca: Arca
        init {
            server.dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    if (request.getHeader("Upgrade")?.equals("websocket", true) == true) {
                        return MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                            override fun onOpen(webSocket: WebSocket, response: okhttp3.Response) { socket = webSocket }
                            override fun onMessage(webSocket: WebSocket, text: String) {
                                val json = arcaJson.parseToJsonElement(text).jsonObject
                                when (json["action"]?.jsonPrimitive?.content) {
                                    "auth" -> webSocket.send("""{"type":"authenticated"}""")
                                    "watch" -> webSocket.send("""{"type":"watch_snapshot","path":${json["path"]},"requestId":${json["requestId"]}}""")
                                }
                            }
                        })
                    }
                    val data = if (request.requestUrl!!.encodedPath.endsWith("/fills")) { reads.incrementAndGet(); page(request) }
                    else """{"object":{"id":"account","realmId":"realm","path":"/account","type":"exchange","status":"active","systemOwned":false,"createdAt":"2026-01-01","updatedAt":"2026-01-01"},"operations":[],"events":[],"deltas":[],"balances":[]}"""
                    return MockResponse().setHeader("Content-Type", "application/json").setBody("""{"success":true,"data":$data}""")
                }
            }
            server.start()
            arca = Arca(token = "test", realmId = "realm", baseUrl = server.url("/").toString().trimEnd('/'))
        }
        override fun close() { arca.ws.shutdown(); server.shutdown() }
    }
    private suspend fun waitFor(condition: () -> Boolean) = withTimeout(5000) { while (!condition()) delay(5) }
    @Test fun paginationRetainsLiveFillDuringSnapshotAndFiltersForeignAccount() = runBlocking {
        Harness().use { h ->
            h.page = { request ->
                if (request.requestUrl!!.queryParameter("cursor") == null) {
                    h.socket!!.send(event("live")); h.socket!!.send(event("foreign", "another"))
                    """{"fills":[${(0 until 200).joinToString(",") { row("row$it") }}],"total":201,"cursor":"next"}"""
                } else """{"fills":[${row("row200")}],"total":201}"""
            }
            val stream = h.arca.watchFills("account", limit = 200)
            waitFor { stream.fills.value.size == 202 }
            assertEquals(2, h.reads.get()); assertFalse(stream.fills.value.any { it.id == "foreign" })
            delay(180); assertEquals(2, h.reads.get(), "healthy watch must stay quiet")
            stream.stop()
        }
    }
    @Test fun emptyWatchRecoversOnQuietAuthenticationAndResyncWithoutPolling() = runBlocking {
        Harness().use { h ->
            val stream = h.arca.watchFills("account"); assertTrue(stream.fills.value.isEmpty())
            h.page = { """{"fills":[${row("quiet")}],"total":1}""" }
            h.socket!!.send("""{"type":"authenticated"}""")
            waitFor { stream.fills.value.any { it.id == "quiet" } }
            h.page = { """{"fills":[${row("gap")}],"total":1}""" }
            h.socket!!.send("""{"type":"stream.resync"}""")
            waitFor { stream.fills.value.any { it.id == "gap" } }
            val reads = h.reads.get(); delay(180); assertEquals(reads, h.reads.get())
            stream.stop(); h.socket!!.send("""{"type":"stream.resync"}"""); delay(80); assertEquals(reads, h.reads.get())
        }
    }
    @Test fun repeatedCursorExhaustsFiniteBudgetThenNewGapCanRecover() = runBlocking {
        Harness().use { h ->
            h.page = { """{"fills":[${row("old")}],"total":10,"cursor":"loop"}""" }
            val stream = h.arca.watchFills("account")
            assertEquals(WatchStreamState.RECONNECTING, stream.state.value); assertEquals(6, h.reads.get())
            delay(180); assertEquals(6, h.reads.get())
            h.page = { """{"fills":[${row("recovered")}],"total":1}""" }
            h.socket!!.send("""{"type":"stream.resync"}""")
            waitFor { stream.fills.value.any { it.id == "recovered" } }
            assertEquals(WatchStreamState.CONNECTED, stream.state.value); stream.stop()
        }
    }
    @Test fun stableFillIdentityKeepsAllPartialExecutionsAndConflicts() {
        fun fill(id: String) = arcaJson.decodeFromString<Fill>(row(id))
        val preview1 = fill("one").copy(operationId = null)
        val preview2 = fill("two").copy(operationId = null)
        val recorded = fill("ledger-one").copy(fillId = "one")
        val merged = mergeWatchedFills(listOf(preview1, preview2), listOf(recorded, recorded, recorded.copy(price = "101")))
        assertEquals(3, merged.size); assertEquals(listOf("two"), merged.filter { it.operationId == null }.map { it.id })
    }
}
