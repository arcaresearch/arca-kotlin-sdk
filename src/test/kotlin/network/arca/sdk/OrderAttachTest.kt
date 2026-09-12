package network.arca.sdk

import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Base64
import java.util.Collections

/**
 * `arca.orderHandle(objectId, operationId)` — attaching to an order this
 * client did not place. The load-bearing property is that attaching is a
 * read: an integration whose backend submits its orders must be able to
 * obtain a handle without placing, cancelling or resizing anything.
 */
class OrderAttachTest {

    private lateinit var server: MockWebServer
    private lateinit var dispatcher: AttachDispatcher

    @BeforeEach
    fun setUp() {
        dispatcher = AttachDispatcher()
        server = MockWebServer()
        server.dispatcher = dispatcher
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    @Test
    fun attachesWithASingleReadAndNoMutation() = runBlocking {
        val arca = makeArca()
        val order = arca.orderHandle(objectId = "obj-1", operationId = "op_place")
        assertEquals("op_place", order.submitted().operation.id.value)
        assertEquals(listOf("GET /api/v1/operations/op_place"), dispatcher.requests())
        arca.close()
    }

    @Test
    fun attachedReceiptUpdatesSharedDisplayBeforeReturning() = runBlocking {
        val arca = makeArca()
        val view = arca.positionView("obj-1")
        view.observe(PositionViewTest.snapshot("0", market = "hl:0:BTC"))
        val update = view.begin("hl:0:BTC", network.arca.sdk.models.OrderSide.BUY)
        val order = arca.orderHandle("obj-1", "op_place")
        order.trackPositionUpdate(update)
        val receipt = order.executionReceipt(2.0)
        assertEquals(receipt.filledSize, view.current.value.positions.first().signedSize)
        assertEquals("op_place", view.current.value.coverage.first().operationId)
        assertEquals("execution", view.current.value.coverage.first().status)
        assertTrue(dispatcher.requests().all { it.startsWith("GET ") })
        arca.close()
    }

    @Test
    fun accountedResolvesOnAnAlreadyRecordedOrder() = runBlocking {
        dispatcher.fillsComplete = mutableListOf(true)
        val arca = makeArca()
        val order = arca.orderHandle(objectId = "obj-1", operationId = "op_place")
        assertEquals(true, order.accounted(timeoutSeconds = 5.0).fillsComplete)
        dispatcher.requests().forEach {
            assertTrue(it.startsWith("GET "), "attach + accounted issued a mutation: $it")
        }
        arca.close()
    }

    @Test
    fun accountedConvergesOnAPendingOrder() = runBlocking {
        dispatcher.fillsComplete = mutableListOf(false, true)
        val arca = makeArca()
        val order = arca.orderHandle(objectId = "obj-1", operationId = "op_place")
        assertEquals(true, order.accounted(timeoutSeconds = 5.0).fillsComplete)
        arca.close()
    }

    @Test
    fun refusesAnotherAccountsOperation() = runBlocking {
        // An id from another account must never resolve into a handle on this one.
        val arca = makeArca()
        val thrown = runCatching { arca.orderHandle(objectId = "obj-other", operationId = "op_place") }.exceptionOrNull()
        assertEquals("ORDER_IDENTITY_MISMATCH", (thrown as? ArcaException.Unknown)?.code, "got $thrown")
        assertEquals(listOf("GET /api/v1/operations/op_place"), dispatcher.requests())
        arca.close()
    }

    @Test
    fun refusesANonOrderOperation() = runBlocking {
        dispatcher.operationType = "transfer"
        val arca = makeArca()
        val thrown = runCatching { arca.orderHandle(objectId = "obj-1", operationId = "op_place") }.exceptionOrNull()
        val unknown = thrown as? ArcaException.Unknown
        assertEquals("ORDER_IDENTITY_MISMATCH", unknown?.code, "got $thrown")
        assertTrue(unknown?.message?.contains("not an order") == true, "message should name the actual type: ${unknown?.message}")
        arca.close()
    }

    @Test
    fun acceptsAnOperationWithNoRecordedAccount() = runBlocking {
        // Absent input is not a mismatch — an older operation may not carry it.
        dispatcher.includeInput = false
        val arca = makeArca()
        val order = arca.orderHandle(objectId = "obj-1", operationId = "op_place")
        assertEquals("op_place", order.submitted().operation.id.value)
        arca.close()
    }

    private fun makeArca(): Arca = Arca(token = fakeJwt(), baseUrl = server.url("/").toString().trimEnd('/'))

    private fun fakeJwt(): String {
        val enc = Base64.getUrlEncoder().withoutPadding()
        val header = enc.encodeToString("""{"alg":"HS256","typ":"JWT"}""".toByteArray())
        val payload = enc.encodeToString("""{"realmId":"rlm_test","sub":"usr_test"}""".toByteArray())
        return "$header.$payload.fakesig"
    }
}

private class AttachDispatcher : Dispatcher() {
    @Volatile var operationType: String = "order"
    @Volatile var includeInput: Boolean = true
    @Volatile var fillsComplete: MutableList<Boolean> = mutableListOf(true)
    private val recorded = Collections.synchronizedList(mutableListOf<String>())

    fun requests(): List<String> = synchronized(recorded) { recorded.toList() }

    private fun nextComplete(): Boolean = synchronized(fillsComplete) {
        val value = fillsComplete.firstOrNull() ?: return true
        if (fillsComplete.size > 1) fillsComplete.removeAt(0)
        value
    }

    override fun dispatch(request: RecordedRequest): MockResponse {
        val path = (request.path ?: "").substringBefore("?")
        // The handle's event capture dials the socket exactly as a placed
        // handle's does. Refusing the upgrade leaves it degraded to reads,
        // which is what these tests exercise; it is not an API request and
        // does not belong in the recorded set.
        if (path.endsWith("/ws")) return MockResponse().setResponseCode(400)
        recorded.add("${request.method} $path")
        return when {
            path == "/api/v1/operations/op_place" -> json(operationBody())
            path == "/api/v1/objects/obj-1/exchange/orders/ord_abc" -> json(orderBody(nextComplete()))
            else -> MockResponse().setResponseCode(404)
        }
    }

    private fun operationBody(): String {
        val input = """{\"exchangeObjectId\":\"obj-1\",\"market\":\"hl:0:BTC\",\"side\":\"buy\",\"size\":\"0.01\",\"orderType\":\"MARKET\"}"""
        val outcome = """{\"orderId\":\"ord_abc\",\"status\":\"filled\",\"filledSize\":\"0.01\",\"avgFillPrice\":\"50000\"}"""
        val inputField = if (includeInput) "\"input\": \"$input\"," else ""
        return """
            {"success":true,"data":{"operation":{
              "id":"op_place","realmId":"rlm_test","path":"/op/order/btc-1",
              "type":"$operationType","state":"completed",
              $inputField
              "outcome":"$outcome",
              "createdAt":"2026-09-11T10:00:01.000000Z","updatedAt":"2026-09-11T10:00:01.000000Z"
            },"events":[],"deltas":[]}}
        """.trimIndent()
    }

    private fun orderBody(complete: Boolean): String = """
        {"success":true,"data":{
          "order":{"id":"ord_abc","accountId":"acc-1","realmId":"rlm_test","market":"hl:0:BTC","side":"buy",
                   "orderType":"MARKET","size":"0.01","filledSize":"0.01","avgFillPrice":"50000",
                   "status":"FILLED","reduceOnly":false,"timeInForce":"IOC","leverage":1,
                   "createdAt":"","updatedAt":""},
          "fills":[],
          "fillsComplete":$complete
        }}
    """.trimIndent()

    private fun json(body: String): MockResponse =
        MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json").setBody(body)
}
