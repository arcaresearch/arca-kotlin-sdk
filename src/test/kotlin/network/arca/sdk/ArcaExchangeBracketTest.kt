package network.arca.sdk

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonObject
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.OrderOperationResponse
import network.arca.sdk.models.OrderSide
import network.arca.sdk.models.OrderType
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Base64
import java.util.Collections

class ArcaExchangeBracketTest {

    private lateinit var server: MockWebServer
    private val posts = Collections.synchronizedList(mutableListOf<JsonObject>())

    @BeforeEach
    fun setUp() {
        posts.clear()
        server = MockWebServer()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val path = request.path ?: ""
                if (request.method == "POST" && path.endsWith("/exchange/orders/batch")) {
                    val bodyStr = request.body.readUtf8()
                    posts.add(arcaJson.parseToJsonElement(bodyStr).jsonObject)
                    return json(BRACKET_ENVELOPE)
                }
                return json("""{"success":true,"data":{}}""")
            }
        }
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    @Test
    fun openWithBracketIssuesOneCallWithEntryAndTriggers() = runBlocking {
        val arca = makeArca()
        val result = arca.openWithBracket(
            path = "/op/bracket/1", objectId = "obj_1", market = "hl:0:BTC",
            side = OrderSide.BUY, size = "0.01", takeProfitPx = "72000", stopLossPx = "58000",
        )
        result.entry.submitted()
        result.takeProfit?.submitted()
        result.stopLoss?.submitted()

        assertEquals(1, posts.size, "exactly one batch POST")
        val body = posts[0]
        assertEquals("normalTpsl", body.str("grouping"))
        val orders = (body["orders"] as JsonArray).map { it.jsonObject }
        assertEquals(3, orders.size)

        assertEquals("buy", orders[0].str("side"))
        assertEquals("MARKET", orders[0].str("orderType"))
        assertEquals("0.01", orders[0].str("size"))
        assertFalse(orders[0].containsKey("reduceOnly"), "entry must not be reduce-only")

        // normalTpsl children are FIXED-SIZE: they default to the entry's size
        // and never carry sizeToMax (that is the whole-position positionTpsl
        // model, which this endpoint rejects).
        val tp = orders.first { it.str("tpsl") == "tp" }
        assertEquals("sell", tp.str("side"))
        assertEquals(true, tp.bool("reduceOnly"))
        assertFalse(tp.containsKey("sizeToMax"), "normalTpsl child must NOT carry sizeToMax")
        assertEquals(true, tp.bool("isTrigger"))
        assertEquals("72000", tp.str("triggerPx"))
        assertEquals("0.01", tp.str("size"), "defaults to the entry size")

        val sl = orders.first { it.str("tpsl") == "sl" }
        assertEquals("sell", sl.str("side"))
        assertEquals("58000", sl.str("triggerPx"))
        assertEquals(true, sl.bool("reduceOnly"))
        assertFalse(sl.containsKey("sizeToMax"), "normalTpsl child must NOT carry sizeToMax")
        assertEquals("0.01", sl.str("size"), "defaults to the entry size")
    }

    @Test
    fun openWithBracketHandlesResolveOwnOrderId() = runBlocking {
        val arca = makeArca()
        val result = arca.openWithBracket(
            path = "/op/bracket/2", objectId = "obj_1", market = "hl:0:BTC",
            side = OrderSide.BUY, size = "0.01", takeProfitPx = "72000", stopLossPx = "58000",
        )
        assertNotNull(result.takeProfit)
        assertNotNull(result.stopLoss)

        assertEquals("ord_entry", orderId(result.entry.submitted()))
        assertEquals("ord_tp", orderId(result.takeProfit!!.submitted()))
        assertEquals("ord_sl", orderId(result.stopLoss!!.submitted()))
    }

    @Test
    fun openWithBracketSizedTakeProfitIsPartial() = runBlocking {
        val arca = makeArca()
        val result = arca.openWithBracket(
            path = "/op/bracket/sized", objectId = "obj_1", market = "hl:0:BTC",
            side = OrderSide.BUY, size = "0.02",
            takeProfitPx = "72000", stopLossPx = "58000", takeProfitSz = "0.01",
        )
        result.entry.submitted()

        val orders = (posts[0]["orders"] as JsonArray).map { it.jsonObject }
        // Explicit takeProfitSz → sized partial close (no sizeToMax).
        val tp = orders.first { it.str("tpsl") == "tp" }
        assertEquals("0.01", tp.str("size"))
        assertFalse(tp.containsKey("sizeToMax"), "sized TP must NOT carry sizeToMax")
        assertEquals(true, tp.bool("reduceOnly"))

        // No explicit size → defaults to the entry's fixed size (never sizeToMax).
        val sl = orders.first { it.str("tpsl") == "sl" }
        assertEquals("0.02", sl.str("size"), "defaults to the entry size")
        assertFalse(sl.containsKey("sizeToMax"), "default SL must NOT carry sizeToMax")
    }

    @Test
    fun openWithBracketOnlyStopLossOmitsTpLeg() = runBlocking {
        val arca = makeArca()
        val result = arca.openWithBracket(
            path = "/op/bracket/3", objectId = "obj_1", market = "hl:0:BTC",
            side = OrderSide.BUY, size = "0.01", stopLossPx = "58000",
        )
        result.entry.submitted()

        val orders = (posts[0]["orders"] as JsonArray).map { it.jsonObject }
        assertEquals(2, orders.size, "entry + sl only")
        assertFalse(orders.any { it.str("tpsl") == "tp" })
        assertNull(result.takeProfit)
        assertNotNull(result.stopLoss)
    }

    @Test
    fun openWithBracketRequiresATrigger() = runBlocking {
        val arca = makeArca()
        val thrown = runCatching {
            arca.openWithBracket(path = "/op/bracket/4", objectId = "obj_1", market = "hl:0:BTC", side = OrderSide.BUY, size = "0.01")
        }.exceptionOrNull()
        assertTrue(thrown is ArcaException.Validation, "got $thrown")
        assertEquals(0, posts.size, "no network call expected")
    }

    @Test
    fun openWithBracketLimitEntryRequiresPrice() = runBlocking {
        val arca = makeArca()
        val thrown = runCatching {
            arca.openWithBracket(
                path = "/op/bracket/5", objectId = "obj_1", market = "hl:0:BTC",
                side = OrderSide.BUY, size = "0.01", orderType = OrderType.LIMIT, takeProfitPx = "72000",
            )
        }.exceptionOrNull()
        assertTrue(thrown is ArcaException.Validation, "got $thrown")
        assertEquals(0, posts.size)
    }

    // MARK: - Helpers

    private fun orderId(resp: OrderOperationResponse): String =
        arcaJson.parseToJsonElement(resp.operation.outcome!!).jsonObject["orderId"]!!.jsonPrimitive.content

    private fun JsonObject.str(key: String): String? = this[key]?.jsonPrimitive?.contentOrNull
    private fun JsonObject.bool(key: String): Boolean? = this[key]?.jsonPrimitive?.booleanOrNull

    private fun json(body: String): MockResponse =
        MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json").setBody(body)

    private fun makeArca(): Arca =
        Arca(token = fakeJwt(), baseUrl = server.url("/").toString().trimEnd('/'))

    private fun fakeJwt(): String {
        val enc = Base64.getUrlEncoder().withoutPadding()
        val header = enc.encodeToString("""{"alg":"HS256","typ":"JWT"}""".toByteArray())
        val payload = enc.encodeToString("""{"realmId":"rlm_test","sub":"usr_test"}""".toByteArray())
        return "$header.$payload.fakesig"
    }
}

private val BRACKET_ENVELOPE: String = buildJsonObject {
    put("success", true)
    putJsonObject("data") {
        putJsonObject("operation") {
            put("id", "op_bracket")
            put("realmId", "rlm_test")
            put("path", "/op/bracket")
            put("type", "order")
            put("state", "completed")
            put(
                "outcome",
                """{"grouping":"normalTpsl","orders":[{"orderId":"ord_entry"},{"orderId":"ord_tp","tpsl":"tp"},{"orderId":"ord_sl","tpsl":"sl"}]}""",
            )
            put("createdAt", "2026-01-01T00:00:00.000000Z")
            put("updatedAt", "2026-01-01T00:00:00.000000Z")
        }
    }
}.toString()
