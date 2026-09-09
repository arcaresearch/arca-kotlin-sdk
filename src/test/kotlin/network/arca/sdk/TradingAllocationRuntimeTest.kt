package network.arca.sdk

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.LeveragePreferenceMode
import network.arca.sdk.models.OrderSide
import network.arca.sdk.models.OrderType
import network.arca.sdk.models.TradingAllocationQuoteRequest
import network.arca.sdk.models.TradingLeverageSelection
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.Base64

class TradingAllocationRuntimeTest {
    @Test
    fun existingLeverageAndMirrorQuoteContracts() = runBlocking {
        MockWebServer().use { server ->
            val update = """{"accountId":"a","market":"gllt:3","leverage":null,"previousLeverage":null,"mode":"fixed","intendedLeverage":8,"revision":"9007199254740993"}"""
            val state = """{"revision":"9007199254740993","preferences":{},"projectionUnavailable":true}"""
            val responses = listOf(update, update, update,
                """{"market":"gllt:3","leverage":null,"marginMode":"cross","mode":"fixed","intendedLeverage":8}""",
                """{"enabled":false,"inputId":"input","allocation":$state,"unavailableReason":"applied_risk_unavailable"}""",
                """{"inputId":"input","market":"gllt:3","referencePrice":"1000","limitPrice":"1010.0","allocation":$state,"maximum":{"revision":"2","maxSize":"1.234567890123456789","maxNotional":"1234.567890123456789"},"affordable":false}""",
                """{"operation":{"id":"op","realmId":"rlm_test","path":"/op/1","type":"order","state":"completed","createdAt":"2026-09-06T00:00:00Z","updatedAt":"2026-09-06T00:00:00Z"}}""",
            )
            val pending = java.util.concurrent.ConcurrentLinkedQueue(responses)
            val httpRequests = java.util.Collections.synchronizedList(mutableListOf<okhttp3.mockwebserver.RecordedRequest>())
            server.dispatcher = object : okhttp3.mockwebserver.Dispatcher() {
                override fun dispatch(request: okhttp3.mockwebserver.RecordedRequest): MockResponse {
                    // The factory now opens its owned stream before POST. That
                    // handshake must not consume a REST fixture from this test.
                    if (request.path?.endsWith("/ws") == true) return MockResponse().setResponseCode(503)
                    httpRequests.add(request)
                    val body = pending.poll() ?: return MockResponse().setResponseCode(500)
                    return MockResponse().setHeader("Content-Type","application/json").setBody("""{"success":true,"data":$body}""")
                }
            }
            server.start()
            val payload = Base64.getUrlEncoder().withoutPadding().encodeToString("""{"realmId":"rlm_test","sub":"user"}""".toByteArray())
            val arca = Arca(token = "e30.$payload.signature", baseUrl = server.url("/").toString().trimEnd('/'))
            try {
            val result = arca.updateLeverage("obj","gllt:3",8,LeveragePreferenceMode.FIXED,"same-retry")
            assertNull(result.leverage)
            assertEquals(8,result.intendedLeverage)
            assertEquals("9007199254740993",result.revision)
            arca.updateLeverage("obj","gllt:3",mode=LeveragePreferenceMode.VENUE_DEFAULT)
            arca.updateLeverage("obj","hl:0:BTC",5)
            val setting = arca.getLeverage("obj","gllt:3").first()
            assertNull(setting.leverage); assertEquals(8,setting.intendedLeverage)
            val read = arca.getTradingAllocation("obj","gllt:3")
            assertFalse(read.enabled); assertTrue(read.allocation.projectionUnavailable); assertNull(read.allocation.projection)
            val quote = arca.quoteTradingAllocation("obj",TradingAllocationQuoteRequest(market="gllt:3",side=OrderSide.BUY,orderType="market",selection=TradingLeverageSelection(LeveragePreferenceMode.FIXED,1)))
            assertEquals("1.234567890123456789",quote.maximum.maxSize); assertEquals(false,quote.affordable)
            arca.placeOrder(path="/op/1",objectId="obj",market="gllt:3",side=OrderSide.BUY,orderType=OrderType.MARKET,size="1",leverage=1,leverageMode=LeveragePreferenceMode.FIXED,slippageBps=100).submitted()
            val requests = httpRequests.toList()
            assertEquals(7, requests.size)
            val first = arcaJson.parseToJsonElement(requests[0].body.readUtf8()).jsonObject
            assertTrue(requests[0].path!!.endsWith("/exchange/leverage"))
            assertEquals("fixed",first["mode"]!!.jsonPrimitive.content)
            assertEquals("same-retry",first["commandId"]!!.jsonPrimitive.content)
            val follow = arcaJson.parseToJsonElement(requests[1].body.readUtf8()).jsonObject
            assertFalse(follow.containsKey("leverage")); assertNotNull(follow["commandId"])
            assertEquals(2,arcaJson.parseToJsonElement(requests[2].body.readUtf8()).jsonObject.size)
            assertTrue(requests[4].path!!.contains("market=gllt"))
            assertTrue(requests[5].path!!.endsWith("/allocation/quote"))
            val placed = arcaJson.parseToJsonElement(requests[6].body.readUtf8()).jsonObject
            assertEquals("fixed",placed["leverageMode"]!!.jsonPrimitive.content)
            assertEquals("100",placed["slippageBps"]!!.jsonPrimitive.content)
            } finally { arca.close() }
        }
    }
}
