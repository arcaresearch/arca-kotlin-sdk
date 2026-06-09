package network.arca.sdk

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.OrderSide
import network.arca.sdk.models.OrderType
import network.arca.sdk.models.SimOrder
import network.arca.sdk.models.TpslType
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

class ArcaExchangeTpslTest {

    private lateinit var server: MockWebServer
    private lateinit var dispatcher: TpslDispatcher

    @BeforeEach
    fun setUp() {
        dispatcher = TpslDispatcher()
        server = MockWebServer()
        server.dispatcher = dispatcher
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    // MARK: - setStopLoss / setTakeProfit

    @Test
    fun setStopLossLongPlacesSellPositionTpsl() = runBlocking {
        dispatcher.positionsBody = envelope("""{"positions":[$longBTC],"total":1}""")
        val arca = makeArca()

        arca.setStopLoss(path = "/op/sl/1", objectId = "obj_1", market = "hl:0:BTC", triggerPx = "55000", isolated = false).submitted()

        assertEquals(1, dispatcher.posts.size)
        val b = dispatcher.posts[0]
        assertEquals("sell", b.str("side"))
        assertEquals("sl", b.str("tpsl"))
        assertEquals(true, b.bool("sizeToMax"))
        assertEquals(true, b.bool("reduceOnly"))
        assertEquals("0", b.str("size"))
        assertEquals(true, b.bool("isTrigger"))
        assertEquals(true, b.bool("isMarket"))
        assertEquals("MARKET", b.str("orderType"))
        assertEquals("55000", b.str("triggerPx"))
        assertEquals(5, b.int("leverage"))
        assertEquals(0, dispatcher.deletes.size)
    }

    @Test
    fun setTakeProfitShortPlacesBuy() = runBlocking {
        dispatcher.positionsBody = envelope("""{"positions":[$shortETH],"total":1}""")
        val arca = makeArca()

        arca.setTakeProfit(path = "/op/tp/1", objectId = "obj_1", market = "hl:0:ETH", triggerPx = "2000", isolated = false).submitted()

        val b = dispatcher.posts[0]
        assertEquals("buy", b.str("side"))
        assertEquals("tp", b.str("tpsl"))
    }

    @Test
    fun setTakeProfitSizedPartial() = runBlocking {
        dispatcher.positionsBody = envelope("""{"positions":[$longBTC],"total":1}""")
        val arca = makeArca()

        arca.setTakeProfit(path = "/op/tp/sized", objectId = "obj_1", market = "hl:0:BTC", triggerPx = "70000", size = "0.25", isolated = false).submitted()

        val b = dispatcher.posts[0]
        assertEquals("0.25", b.str("size"))
        assertFalse(b.containsKey("sizeToMax"), "sized trigger must NOT carry sizeToMax")
        assertEquals(true, b.bool("reduceOnly"))
    }

    @Test
    fun setStopLossNoPositionThrowsNotFound() = runBlocking {
        dispatcher.positionsBody = envelope("""{"positions":[],"total":0}""")
        val arca = makeArca()

        val handle = arca.setStopLoss(path = "/op/sl/2", objectId = "obj_1", market = "hl:0:BTC", triggerPx = "55000", isolated = false)
        val thrown = runCatching { handle.submitted() }.exceptionOrNull()
        assertTrue(thrown is ArcaException.NotFound, "got $thrown")
        assertEquals(0, dispatcher.posts.size)
    }

    @Test
    fun replaceCancelsExisting() = runBlocking {
        dispatcher.positionsBody = envelope("""{"positions":[$longBTC],"total":1}""")
        dispatcher.ordersBody = envelope("""{"orders":[${restingSL("ord_old_sl")}],"total":1}""")
        val arca = makeArca()

        arca.setStopLoss(path = "/op/sl/3", objectId = "obj_1", market = "hl:0:BTC", triggerPx = "54000", isolated = false).submitted()

        assertEquals(listOf("ord_old_sl"), dispatcher.deletes)
        assertEquals(1, dispatcher.posts.size)
    }

    @Test
    fun noReplaceSkipsCancel() = runBlocking {
        dispatcher.positionsBody = envelope("""{"positions":[$longBTC],"total":1}""")
        dispatcher.ordersBody = envelope("""{"orders":[${restingSL("ord_old_sl")}],"total":1}""")
        val arca = makeArca()

        arca.setStopLoss(path = "/op/sl/4", objectId = "obj_1", market = "hl:0:BTC", triggerPx = "54000", replace = false, isolated = false).submitted()

        assertEquals(0, dispatcher.deletes.size, "replace=false must not cancel")
        assertEquals(1, dispatcher.posts.size)
    }

    @Test
    fun triggerLimitRequiresLimitPrice() = runBlocking {
        dispatcher.positionsBody = envelope("""{"positions":[$longBTC],"total":1}""")
        val arca = makeArca()

        val handle = arca.setStopLoss(path = "/op/sl/5", objectId = "obj_1", market = "hl:0:BTC", triggerPx = "54000", isMarket = false)
        val thrown = runCatching { handle.submitted() }.exceptionOrNull()
        assertTrue(thrown is ArcaException.Validation, "got $thrown")
        assertEquals(0, dispatcher.posts.size)
    }

    @Test
    fun triggerLimitUsesLimitPrice() = runBlocking {
        dispatcher.positionsBody = envelope("""{"positions":[$longBTC],"total":1}""")
        val arca = makeArca()

        arca.setStopLoss(
            path = "/op/sl/6", objectId = "obj_1", market = "hl:0:BTC", triggerPx = "54000",
            isMarket = false, limitPrice = "53900", isolated = false,
        ).submitted()

        val b = dispatcher.posts[0]
        assertEquals("LIMIT", b.str("orderType"))
        assertEquals("53900", b.str("price"))
        assertEquals(false, b.bool("isMarket"))
    }

    @Test
    fun infersIsolatedFromMeta() = runBlocking {
        dispatcher.positionsBody = envelope("""{"positions":[$longCL],"total":1}""")
        dispatcher.metaBody = envelope("""{"universe":[{"name":"hl:1:CL","symbol":"CL","exchange":"hl","index":0,"szDecimals":2,"maxLeverage":5,"onlyIsolated":true}]}""")
        val arca = makeArca()

        arca.setStopLoss(path = "/op/sl/7", objectId = "obj_1", market = "hl:1:CL", triggerPx = "60").submitted()

        assertEquals(true, dispatcher.posts[0].bool("isolated"))
    }

    // MARK: - setPositionTpsl

    @Test
    fun setPositionTpslPlacesBothLegs() = runBlocking {
        dispatcher.positionsBody = envelope("""{"positions":[$longBTC],"total":1}""")
        dispatcher.metaBody = btcMeta()
        val arca = makeArca()

        val result = arca.setPositionTpsl(path = "/op/tpsl/1", objectId = "obj_1", market = "hl:0:BTC", stopLossPx = "54000", takeProfitPx = "70000")
        assertNotNull(result.stopLoss)
        assertNotNull(result.takeProfit)

        assertEquals(2, dispatcher.posts.size)
        assertEquals("sl", dispatcher.posts[0].str("tpsl"))
        assertEquals("/op/tpsl/1/sl", dispatcher.posts[0].str("path"))
        assertEquals("tp", dispatcher.posts[1].str("tpsl"))
        assertEquals("/op/tpsl/1/tp", dispatcher.posts[1].str("path"))
    }

    @Test
    fun setPositionTpslRequiresOnePrice() = runBlocking {
        val arca = makeArca()
        val thrown = runCatching { arca.setPositionTpsl(path = "/op/tpsl/2", objectId = "obj_1", market = "hl:0:BTC") }.exceptionOrNull()
        assertTrue(thrown is ArcaException.Validation, "got $thrown")
    }

    @Test
    fun setPositionTpslSharesOcoGroupId() = runBlocking {
        dispatcher.positionsBody = envelope("""{"positions":[$longBTC],"total":1}""")
        dispatcher.metaBody = btcMeta()
        val arca = makeArca()

        arca.setPositionTpsl(path = "/op/tpsl/oco", objectId = "obj_1", market = "hl:0:BTC", stopLossPx = "54000", takeProfitPx = "70000")

        assertEquals(2, dispatcher.posts.size)
        val slGroup = dispatcher.posts[0].str("ocoGroupId")
        val tpGroup = dispatcher.posts[1].str("ocoGroupId")
        assertNotNull(slGroup)
        assertFalse(slGroup.isNullOrEmpty(), "ocoGroupId must be non-empty")
        assertEquals(slGroup, tpGroup, "both legs must share one ocoGroupId")
    }

    @Test
    fun setPositionTpslExplicitOcoGroupId() = runBlocking {
        dispatcher.positionsBody = envelope("""{"positions":[$longBTC],"total":1}""")
        dispatcher.metaBody = btcMeta()
        val arca = makeArca()

        arca.setPositionTpsl(path = "/op/tpsl/oco2", objectId = "obj_1", market = "hl:0:BTC", stopLossPx = "54000", takeProfitPx = "70000", ocoGroupId = "oco_explicit")

        assertEquals("oco_explicit", dispatcher.posts[0].str("ocoGroupId"))
        assertEquals("oco_explicit", dispatcher.posts[1].str("ocoGroupId"))
    }

    @Test
    fun setPositionTpslSizedSkipsAutoOco() = runBlocking {
        dispatcher.positionsBody = envelope("""{"positions":[$longBTC],"total":1}""")
        dispatcher.metaBody = btcMeta()
        val arca = makeArca()

        arca.setPositionTpsl(path = "/op/tpsl/sized", objectId = "obj_1", market = "hl:0:BTC", stopLossPx = "54000", takeProfitPx = "70000", takeProfitSz = "0.25")

        assertEquals(2, dispatcher.posts.size)
        assertFalse(dispatcher.posts[0].containsKey("ocoGroupId"), "sized legs must NOT be auto-OCO-linked")
        assertFalse(dispatcher.posts[1].containsKey("ocoGroupId"), "sized legs must NOT be auto-OCO-linked")
        assertEquals("sl", dispatcher.posts[0].str("tpsl"))
        assertEquals(true, dispatcher.posts[0].bool("sizeToMax"))
        assertEquals("tp", dispatcher.posts[1].str("tpsl"))
        assertEquals("0.25", dispatcher.posts[1].str("size"))
        assertFalse(dispatcher.posts[1].containsKey("sizeToMax"), "sized TP must NOT carry sizeToMax")
    }

    @Test
    fun setPositionTpslSizedRespectsExplicitOco() = runBlocking {
        dispatcher.positionsBody = envelope("""{"positions":[$longBTC],"total":1}""")
        dispatcher.metaBody = btcMeta()
        val arca = makeArca()

        arca.setPositionTpsl(
            path = "/op/tpsl/sizedoco", objectId = "obj_1", market = "hl:0:BTC",
            stopLossPx = "54000", takeProfitPx = "70000", stopLossSz = "0.25", takeProfitSz = "0.25", ocoGroupId = "oco_forced",
        )

        assertEquals("oco_forced", dispatcher.posts[0].str("ocoGroupId"))
        assertEquals("oco_forced", dispatcher.posts[1].str("ocoGroupId"))
    }

    @Test
    fun placeOrderForwardsOcoGroupId() = runBlocking {
        val arca = makeArca()

        arca.placeOrder(
            path = "/op/place/oco", objectId = "obj_1", market = "hl:0:BTC",
            side = OrderSide.SELL, orderType = OrderType.MARKET, size = "0", ocoGroupId = "oco_grp_99",
        ).submitted()

        assertEquals(1, dispatcher.posts.size)
        assertEquals("oco_grp_99", dispatcher.posts[0].str("ocoGroupId"))
    }

    @Test
    fun simOrderDecodesOcoAndCancelReason() {
        val json = """
            {"id":"ord_1","market":"hl:0:BTC","side":"sell","orderType":"MARKET",
             "size":"0","filledSize":"0","status":"CANCELLED","reduceOnly":true,
             "timeInForce":"GTC","leverage":5,
             "ocoGroupId":"oco_abc","cancelReason":"sibling_filled"}
        """.trimIndent()
        val order = arcaJson.decodeFromString(SimOrder.serializer(), json)
        assertEquals("oco_abc", order.ocoGroupId)
        assertEquals("sibling_filled", order.cancelReason)
    }

    // MARK: - clearPositionTpsl

    @Test
    fun clearPositionTpslCancelsBothLegs() = runBlocking {
        dispatcher.ordersBody = envelope(
            """{"orders":[${restingSL("ord_sl")},${restingTP("ord_tp")},${sizedSL("ord_other")},${restingSLForCoin("ord_eth", "hl:0:ETH")}],"total":4}""",
        )
        val arca = makeArca()

        val cleared = arca.clearPositionTpsl(path = "/op/clear/1", objectId = "obj_1", market = "hl:0:BTC")
        assertEquals(2, cleared.size, "only unsized orders for hl:0:BTC")
        assertEquals(setOf("ord_sl", "ord_tp"), dispatcher.deletes.toSet())
    }

    @Test
    fun clearPositionTpslFilterByLeg() = runBlocking {
        dispatcher.ordersBody = envelope("""{"orders":[${restingSL("ord_sl")},${restingTP("ord_tp")}],"total":2}""")
        val arca = makeArca()

        val cleared = arca.clearPositionTpsl(path = "/op/clear/2", objectId = "obj_1", market = "hl:0:BTC", tpsl = TpslType.STOP_LOSS)
        assertEquals(1, cleared.size)
        assertEquals("ord_sl", cleared.first().id.value)
        assertEquals(listOf("ord_sl"), dispatcher.deletes)
    }

    // MARK: - Fixtures

    private val longBTC = """{"id":"pos_1","market":"hl:0:BTC","side":"long","size":"0.5","entryPrice":"60000","leverage":5,"marginUsed":"6000"}"""
    private val shortETH = """{"id":"pos_2","market":"hl:0:ETH","side":"short","size":"2","entryPrice":"2500","leverage":3,"marginUsed":"1666"}"""
    private val longCL = """{"id":"pos_cl","market":"hl:1:CL","side":"long","size":"1","entryPrice":"60","leverage":2,"marginUsed":"30"}"""

    private fun restingSL(id: String) = restingSLForCoin(id, "hl:0:BTC")
    private fun restingSLForCoin(id: String, market: String) =
        """{"id":"$id","market":"$market","side":"sell","orderType":"MARKET","size":"0","filledSize":"0","status":"WAITING_FOR_TRIGGER","reduceOnly":true,"timeInForce":"GTC","leverage":5,"tpsl":"sl","sizeToMax":true}"""

    private fun restingTP(id: String) =
        """{"id":"$id","market":"hl:0:BTC","side":"sell","orderType":"MARKET","size":"0","filledSize":"0","status":"WAITING_FOR_TRIGGER","reduceOnly":true,"timeInForce":"GTC","leverage":5,"tpsl":"tp","sizeToMax":true}"""

    private fun sizedSL(id: String) =
        """{"id":"$id","market":"hl:0:BTC","side":"sell","orderType":"MARKET","size":"0.5","filledSize":"0","status":"WAITING_FOR_TRIGGER","reduceOnly":true,"timeInForce":"GTC","leverage":5,"tpsl":"sl","sizeToMax":false}"""

    private fun btcMeta() =
        envelope("""{"universe":[{"name":"hl:0:BTC","symbol":"BTC","exchange":"hl","index":0,"szDecimals":5,"maxLeverage":50,"onlyIsolated":false}]}""")

    private fun envelope(data: String) = """{"success":true,"data":$data}"""

    private fun JsonObject.str(key: String): String? = this[key]?.jsonPrimitive?.contentOrNull
    private fun JsonObject.bool(key: String): Boolean? = this[key]?.jsonPrimitive?.booleanOrNull
    private fun JsonObject.int(key: String): Int? = this[key]?.jsonPrimitive?.intOrNull

    private fun makeArca(): Arca = Arca(token = fakeJwt(), baseUrl = server.url("/").toString().trimEnd('/'))

    private fun fakeJwt(): String {
        val enc = Base64.getUrlEncoder().withoutPadding()
        val header = enc.encodeToString("""{"alg":"HS256","typ":"JWT"}""".toByteArray())
        val payload = enc.encodeToString("""{"realmId":"rlm_test","sub":"usr_test"}""".toByteArray())
        return "$header.$payload.fakesig"
    }
}

private class TpslDispatcher : Dispatcher() {
    @Volatile var positionsBody = """{"success":true,"data":{"positions":[],"total":0}}"""
    @Volatile var ordersBody = """{"success":true,"data":{"orders":[],"total":0}}"""
    @Volatile var metaBody = """{"success":true,"data":{"universe":[]}}"""

    val posts: MutableList<JsonObject> = Collections.synchronizedList(mutableListOf())
    val deletes: MutableList<String> = Collections.synchronizedList(mutableListOf())

    private val opEnvelope =
        """{"success":true,"data":{"operation":{"id":"op_1","realmId":"rlm_test","path":"/op/x","type":"order","state":"completed","createdAt":"2026-01-01T00:00:00.000000Z","updatedAt":"2026-01-01T00:00:00.000000Z"}}}"""

    override fun dispatch(request: RecordedRequest): MockResponse {
        val method = request.method ?: "GET"
        val pathOnly = (request.path ?: "").substringBefore("?")
        return when {
            method == "GET" && pathOnly.endsWith("/exchange/positions") -> json(positionsBody)
            method == "GET" && pathOnly.endsWith("/exchange/market/meta") -> json(metaBody)
            method == "GET" && pathOnly.endsWith("/exchange/orders") -> json(ordersBody)
            method == "DELETE" && pathOnly.contains("/exchange/orders/") -> {
                deletes.add(pathOnly.substringAfter("/exchange/orders/"))
                json(opEnvelope)
            }
            method == "POST" && pathOnly.endsWith("/exchange/orders") -> {
                posts.add(arcaJson.parseToJsonElement(request.body.readUtf8()).jsonObject)
                json(opEnvelope)
            }
            else -> json("""{"success":true,"data":{}}""")
        }
    }

    private fun json(body: String): MockResponse =
        MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json").setBody(body)
}
