package network.arca.sdk

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.collect
import kotlinx.serialization.json.*
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.OrderSide
import network.arca.sdk.models.OrderType
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.mockwebserver.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.Base64
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class OrderLifecycleHandleTest {
    private val requested = "9007199254740993.123456789"
    private fun view(complete: Boolean = false): JsonObject = buildJsonObject {
        putJsonObject("intent") {
            put("realmId","realm"); put("objectId","account"); put("operationId","original"); put("leg","0"); put("venue","gll-testnet"); put("venueAccountId","123"); put("market","gllt:3")
            put("requestedSize",requested); put("orderType","MARKET"); put("side","buy"); put("timeInForce","GTC"); put("executionTimeInForce","IOC")
            put("isTrigger",false); put("isMarketTrigger",false); put("sizeToMax",false); put("reduceOnly",false)
        }
        put("venueOrderId","3:order"); put("submission","accepted"); put("working",false); put("execution","partial"); put("terminal",true)
        put("executedSize","3.123456789"); put("executionQuantityFinal",true); put("requestedSizeKnown",true); put("remainingSize","9007199254740990"); put("remainingDisposition","cancelled")
        put("accountedSize",if(complete) "3.123456789" else "0"); put("accountingComplete",complete); put("averagePrice","2000.000000001"); put("averagePriceFinal",complete); put("recoveryRequired",false)
        putJsonObject("executionReceipt") {
            put("objectId","account"); put("operationId","original"); put("leg","0"); put("market","gllt:3"); put("orderId","3:order"); put("status","FILLED")
            put("filledSize","3.123456789"); put("requestedSize",requested); put("remainingSize","9007199254740990"); put("executionState","partial"); put("fulfillmentState","partial"); put("remainingDisposition","cancelled")
            put("avgFillPrice","2000.000000001"); put("averagePriceFinal",complete); put("averagePriceSource",if(complete) "ledger_vwap" else "venue_aggregate"); put("fillsComplete",complete)
        }
    }
    private inner class Harness(val lost: Boolean = false): AutoCloseable {
        val server=MockWebServer()
        val posts=AtomicInteger(); val detailReads=AtomicInteger(); val pathReads=AtomicInteger()
        val requests=CopyOnWriteArrayList<JsonObject>()
        val socket=AtomicReference<WebSocket>()
        val arca: Arca
        init {
            server.dispatcher=object: Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    if(request.getHeader("Upgrade")?.lowercase()=="websocket") return MockResponse().withWebSocketUpgrade(object: WebSocketListener() {
                        override fun onOpen(webSocket: WebSocket,response: Response) { socket.set(webSocket) }
                        override fun onMessage(webSocket: WebSocket,text: String) {
                            val json=arcaJson.parseToJsonElement(text).jsonObject; requests+=json
                            when(json["action"]?.jsonPrimitive?.content) {
                                "auth" -> webSocket.send("""{"type":"authenticated"}""")
                                "watch" -> webSocket.send(buildJsonObject { put("type","watch_snapshot"); put("path",json["path"]!!); json["requestId"]?.let { put("requestId",it) } }.toString())
                                "ping" -> webSocket.send("""{"type":"pong"}""")
                            }
                        }
                    })
                    val path=request.requestUrl!!.encodedPath
                    if(request.method=="POST" && (path.endsWith("/exchange/orders") || path.endsWith("/exchange/orders/batch"))) {
                        posts.incrementAndGet()
                        if(lost) return MockResponse().setResponseCode(504).setBody("""{"success":false,"error":{"code":"GATEWAY_TIMEOUT","message":"response lost"}}""")
                        return envelope("""{"operation":{"id":"original","realmId":"realm","path":"/alice/original","type":"order","state":"completed","actorType":"user","createdAt":"2026-09-09T00:00:00Z","updatedAt":"2026-09-09T00:00:00Z"}}""")
                    }
                    if(path.endsWith("/exchange/order-lifecycle")) {
                        pathReads.incrementAndGet(); assertEquals("/alice/original",request.requestUrl!!.queryParameter("operationPath"))
                        return envelope(buildJsonObject { put("lifecycle",view()) }.toString())
                    }
                    if(path.endsWith("/exchange/orders/3:order")) {
                        detailReads.incrementAndGet()
                        return envelope("""{"order":{"id":"3:order","accountId":"123","realmId":"realm","market":"gllt:3","side":"buy","orderType":"MARKET","size":"$requested","filledSize":"3.123456789","avgFillPrice":"2000.000000001","status":"FILLED","reduceOnly":false,"timeInForce":"GTC","leverage":1,"createdAt":"2026-09-09T00:00:00Z","updatedAt":"2026-09-09T00:00:00Z"},"fills":[],"fillsComplete":true}""")
                    }
                    return MockResponse().setResponseCode(404)
                }
            }
            server.start()
            val enc=Base64.getUrlEncoder().withoutPadding()
            val header=enc.encodeToString("{}".toByteArray()); val payload=enc.encodeToString("""{"realmId":"realm","sub":"user"}""".toByteArray())
            arca=Arca(token="$header.$payload.signature",baseUrl=server.url("/").toString().trimEnd('/'))
        }
        fun watches(): List<JsonObject> = requests.filter { it["action"]?.jsonPrimitive?.content=="watch_order_lifecycle" }
        fun push(complete: Boolean) = push(view(complete))
        fun push(snapshot: JsonObject) {
            val request=watches().last()
            socket.get().send(buildJsonObject {
                put("type","order.lifecycle.updated"); put("watchId",request["watchId"]!!); put("requestId",request["requestId"]!!)
                put("realmId","realm"); put("objectId","account"); put("operationId","original"); put("leg",request["leg"]!!); put("lifecycle",snapshot)
            }.toString())
        }
        override fun close() { arca.close(); server.shutdown() }
    }
    private fun envelope(data: String): MockResponse = MockResponse().setHeader("Content-Type","application/json").setBody("""{"success":true,"data":$data}""")
    private suspend fun waitFor(condition: () -> Boolean) { withTimeout(3000) { while(!condition()) delay(5) } }

    @Test fun factoryUsesOriginalReceiptAndWaitsForAccountingAfterLostHttp(): Unit = runBlocking {
        for(lost in listOf(false,true)) Harness(lost).use { h ->
            val handle=h.arca.placeOrder(path="/alice/original",objectId="account",market="gllt:3",side=OrderSide.BUY,orderType=OrderType.MARKET,size=requested)
            val receipt=async { handle.executionReceipt(3.0) }
            waitFor { h.watches().size==1 }; h.push(false)
            val first=receipt.await(); assertEquals(requested,first.requestedSize); assertFalse(first.fillsComplete); assertFalse(first.averagePriceFinal)
            val filled=async { handle.filled(3.0) }
            waitFor { h.watches().size==2 }; h.push(false); delay(50)
            assertFalse(filled.isCompleted); assertEquals(0,h.detailReads.get())
            h.push(true); assertEquals(true,filled.await().fillsComplete)
            assertEquals(1,h.posts.get()); assertEquals(1,h.detailReads.get())
            waitFor { h.requests.count { it["action"]?.jsonPrimitive?.content=="unwatch_order_lifecycle" }==2 }
            if(lost) assertEquals(2,h.pathReads.get())
        }
    }

    @Test fun bracketStopLossFollowsItsOriginalLeg(): Unit = runBlocking {
        Harness().use { h ->
            val bracket=h.arca.openWithBracket(path="/alice/original",objectId="account",market="gllt:3",side=OrderSide.BUY,size="10",takeProfitPx="2100",stopLossPx="1900")
            val watch=bracket.stopLoss!!.lifecycleUpdates()
            try { waitFor { h.watches().size==1 }; assertEquals("2",h.watches()[0]["leg"]!!.jsonPrimitive.content); assertEquals(1,h.posts.get()) }
            finally { watch.stop() }
        }
    }
    @Test fun fillStreamRecoversCanonicalFillsOnceAfterLostHttp(): Unit = runBlocking {
        Harness(true).use { h ->
            val handle=h.arca.placeOrder(path="/alice/original",objectId="account",market="gllt:3",side=OrderSide.BUY,orderType=OrderType.MARKET,size="10")
            val received=CopyOnWriteArrayList<String>()
            val consumer=async { handle.fills(3.0).collect { assertEquals("0.123456789",it.fee); received+=it.id.value } }
            waitFor { h.watches().size==1 }
            fun fill(id:String,size:String)=buildJsonObject {
                put("id",id);put("size",size);put("price","2000.000000001");put("fee","0.123456789");put("orderId","3:order");put("realmId","realm")
                put("objectId","account");put("operationId","original");put("leg","0");put("accountId","123");put("market","gllt:3");put("side","buy")
            }
            val partial=JsonObject(view().toMutableMap().apply { put("accountedSize",JsonPrimitive("1"));put("committedFills",JsonArray(listOf(fill("a","1")))) })
            h.push(partial);waitFor { received.toList()==listOf("a") };h.push(partial)
            h.push(JsonObject(view(true).toMutableMap().apply { put("committedFills",JsonArray(listOf(fill("a","1"),fill("b","1"),fill("c","1.123456789")))) }))
            consumer.await();assertEquals(listOf("a","b","c"),received.toList());assertEquals(1,h.posts.get());assertEquals(0,h.detailReads.get())
            waitFor { h.requests.count { it["action"]?.jsonPrimitive?.content=="unwatch_order_lifecycle" }==1 }
        }
    }

    @Test fun fillStreamCancellationReleasesPendingWatch(): Unit = runBlocking {
        Harness().use { h ->
            val handle=h.arca.placeOrder(path="/alice/original",objectId="account",market="gllt:3",side=OrderSide.BUY,orderType=OrderType.MARKET,size="10")
            val consumer=launch { handle.fills(3.0).collect { fail<Unit>("no committed fills were sent") } }
            waitFor { h.watches().size==1 };consumer.cancelAndJoin()
            waitFor { h.requests.count { it["action"]?.jsonPrimitive?.content=="unwatch_order_lifecycle" }==1 }
            assertEquals(1,h.posts.get());assertEquals(0,h.detailReads.get())
        }
    }

    suspend fun verifyIndependentBracketReceipts(): Unit = coroutineScope {
        Harness().use { h ->
            val bracket=h.arca.openWithBracket(path="/alice/original",objectId="account",market="gllt:3",side=OrderSide.BUY,size="40",takeProfitPx="2100",takeProfitSz="10")
            fun snapshot(leg:String,order:String,size:String,filled:String,remaining:String,fulfillment:String):JsonObject {
                val root=view().toMutableMap()
                root["intent"]=JsonObject(root["intent"]!!.jsonObject.toMutableMap().apply { put("leg",JsonPrimitive(leg));put("requestedSize",JsonPrimitive(size));put("side",JsonPrimitive(if(leg=="0") "buy" else "sell")) })
                root["executionReceipt"]=JsonObject(root["executionReceipt"]!!.jsonObject.toMutableMap().apply {
                    put("leg",JsonPrimitive(leg));put("orderId",JsonPrimitive(order));put("requestedSize",JsonPrimitive(size));put("filledSize",JsonPrimitive(filled));put("remainingSize",JsonPrimitive(remaining));put("fulfillmentState",JsonPrimitive(fulfillment))
                })
                root["venueOrderId"]=JsonPrimitive(order);root["executedSize"]=JsonPrimitive(filled);root["remainingSize"]=JsonPrimitive(remaining)
                return JsonObject(root)
            }
            val entry=async { bracket.entry.executionReceipt(3.0) };waitFor {h.watches().size==1};assertEquals("0",h.watches().last()["leg"]!!.jsonPrimitive.content)
            h.push(snapshot("0","entry","40","4.041","35.959","partial"));val first=entry.await();assertEquals("40",first.requestedSize);assertEquals("4.041",first.filledSize)
            val child=async { bracket.takeProfit!!.executionReceipt(3.0) };waitFor {h.watches().size==2};assertEquals("1",h.watches().last()["leg"]!!.jsonPrimitive.content)
            h.push(snapshot("1","tp","10","10","0","full"));val second=child.await();assertEquals("tp",second.orderId);assertEquals("10",second.requestedSize);assertEquals("10",second.filledSize)
            assertEquals(1,h.posts.get());assertEquals(0,h.detailReads.get())
        }
    }

}
