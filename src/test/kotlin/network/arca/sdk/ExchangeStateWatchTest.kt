package network.arca.sdk

import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger

/**
 * Verifies `watchExchangeState` applies inline structural state from an
 * `exchange.updated` event without issuing a second `/exchange/state` refetch.
 */
class ExchangeStateWatchTest {

    private lateinit var server: MockWebServer
    private lateinit var dispatcher: StateDispatcher

    @BeforeEach
    fun setUp() {
        dispatcher = StateDispatcher()
        server = MockWebServer()
        server.dispatcher = dispatcher
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    @Test
    fun usesInlineStateWhenPendingIntentsPresent() = runBlocking {
        val arca = makeArca()
        val stream = arca.watchExchangeState(objectId = "obj_1")
        assertEquals(1, dispatcher.stateRequestCount, "initial state fetch")

        // Subscribe before injecting so the watch's notification collector is live.
        val seen = async {
            withTimeoutOrNull(2_000) { stream.exchangeState.first { it?.pendingIntents?.size == 1 } }
        }
        delay(150)
        arca.ws.injectMessage(EXCHANGE_UPDATED_INLINE)

        val state = seen.await()
        assertNotNull(state, "inline exchange state never applied")
        assertEquals(1, state!!.pendingIntents?.size)
        assertEquals(1, dispatcher.stateRequestCount, "inline state must not trigger a refetch")

        stream.stop()
        arca.close()
    }

    // MARK: - Helpers

    private fun makeArca(): Arca = Arca(token = fakeJwt(), baseUrl = server.url("/").toString().trimEnd('/'))

    private fun fakeJwt(): String {
        val enc = Base64.getUrlEncoder().withoutPadding()
        val header = enc.encodeToString("""{"alg":"HS256","typ":"JWT"}""".toByteArray())
        val payload = enc.encodeToString("""{"realmId":"rlm_test","sub":"usr_test"}""".toByteArray())
        return "$header.$payload.fakesig"
    }

    private companion object {
        val EXCHANGE_UPDATED_INLINE = """
            {
              "type": "exchange.updated",
              "entityId": "obj_1",
              "entityPath": "/exchanges/main",
              "exchangeState": {
                "account": {"id":"act_1","realmId":"rlm_test","name":"main","createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z"},
                "marginSummary": {"equity":"1200","initialMarginUsed":"0","maintenanceMarginRequired":"0","availableToWithdraw":"1200","totalNtlPos":"0","totalUnrealizedPnl":"0"},
                "positions": [],
                "openOrders": [],
                "pendingIntents": [
                  {"operationId":"op_1","operationPath":"/ops/1","market":"hl:0:BTC","side":"buy","size":"0.1","orderType":"MARKET","reduceOnly":false,"createdAt":"2026-01-01T00:00:00Z"}
                ]
              }
            }
        """.trimIndent()
    }
}

private class StateDispatcher : Dispatcher() {
    private val stateCount = AtomicInteger(0)
    val stateRequestCount: Int get() = stateCount.get()

    override fun dispatch(request: RecordedRequest): MockResponse {
        val path = (request.path ?: "").substringBefore("?")
        return when {
            path.endsWith("/exchange/state") -> {
                stateCount.incrementAndGet()
                json(STATE_BODY)
            }
            path.endsWith("/objects/obj_1") -> json(OBJECT_DETAIL)
            else -> MockResponse().setResponseCode(404)
        }
    }

    private fun json(body: String): MockResponse =
        MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json").setBody(body)

    private companion object {
        val OBJECT_DETAIL = """
            {"success":true,"data":{"object":{
              "id":"obj_1","realmId":"rlm_test","path":"/exchanges/main","type":"exchange",
              "denomination":"USD","status":"active","systemOwned":false,
              "createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z"
            }}}
        """.trimIndent()

        val STATE_BODY = """
            {"success":true,"data":{
              "account": {"id":"act_1","realmId":"rlm_test","name":"main","createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z"},
              "marginSummary": {"equity":"1000","initialMarginUsed":"0","maintenanceMarginRequired":"0","availableToWithdraw":"1000","totalNtlPos":"0","totalUnrealizedPnl":"0"},
              "positions": [],
              "openOrders": [],
              "pendingIntents": []
            }}
        """.trimIndent()
    }
}
