package network.arca.sdk

import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Base64

/**
 * Verifies `getEquityHistory` / `getPnlHistory` request the v2 `target`/`kind`
 * query shape (never the legacy `prefix`) and normalize the response.
 */
class AggregationHistoryTest {

    private lateinit var server: MockWebServer
    private lateinit var dispatcher: HistoryDispatcher

    private val from = "2026-01-01T00:00:00Z"
    private val to = "2026-01-01T01:00:00Z"

    @BeforeEach
    fun setUp() {
        dispatcher = HistoryDispatcher()
        server = MockWebServer()
        server.dispatcher = dispatcher
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    @Test
    fun getEquityHistoryRequestsV2TargetFormatAndNormalizesResponse() = runBlocking {
        dispatcher.responseBody = """
            {"success":true,"data":{"resolution":"5m","points":[
              {"ts":"$from","equityUsd":"1000.00"},
              {"ts":"$to","equityUsd":"1250.00"}
            ]}}
        """.trimIndent()
        val arca = makeArca()

        val result = arca.getEquityHistory(path = "/users/alice/main", from = from, to = to, points = 2)

        assertTrue(dispatcher.lastPath?.endsWith("/objects/aggregate/history") == true, "got ${dispatcher.lastPath}")
        assertEquals("/users/alice/main", dispatcher.lastQuery["target"])
        assertEquals("path", dispatcher.lastQuery["kind"])
        assertNull(dispatcher.lastQuery["prefix"])
        assertEquals("/users/alice/main", result.prefix)
        assertEquals(2, result.points)
        assertEquals(from, result.equityPoints.first().timestamp)
        assertEquals("1000.00", result.equityPoints.first().equityUsd)
    }

    @Test
    fun getPnlHistoryRequestsV2TargetFormatAndNormalizesResponse() = runBlocking {
        dispatcher.responseBody = """
            {"success":true,"data":{"resolution":"5m","startEquityUsd":"1000.00","points":[
              {"ts":"$from","equityUsd":"1000.00","pnlUsd":"0.00","valueUsd":"1000.00"},
              {"ts":"$to","equityUsd":"1250.00","pnlUsd":"250.00","valueUsd":"1250.00"}
            ]}}
        """.trimIndent()
        val arca = makeArca()

        val result = arca.getPnlHistory(path = "/users/alice/main", from = from, to = to, points = 2)

        assertTrue(dispatcher.lastPath?.endsWith("/objects/pnl/history") == true, "got ${dispatcher.lastPath}")
        assertEquals("/users/alice/main", dispatcher.lastQuery["target"])
        assertEquals("path", dispatcher.lastQuery["kind"])
        assertNull(dispatcher.lastQuery["prefix"])
        assertEquals("1000.00", result.startingEquityUsd)
        assertEquals(2, result.points)
        assertEquals(to, result.pnlPoints.last().timestamp)
        assertEquals("250.00", result.pnlPoints.last().pnlUsd)
        assertEquals("1250.00", result.pnlPoints.last().valueUsd)
    }

    // MARK: - Helpers

    private fun makeArca(): Arca = Arca(token = fakeJwt(), baseUrl = server.url("/").toString().trimEnd('/'))

    private fun fakeJwt(): String {
        val enc = Base64.getUrlEncoder().withoutPadding()
        val header = enc.encodeToString("""{"alg":"HS256","typ":"JWT"}""".toByteArray())
        val payload = enc.encodeToString("""{"realmId":"rlm_test","sub":"usr_test"}""".toByteArray())
        return "$header.$payload.fakesig"
    }
}

private class HistoryDispatcher : Dispatcher() {
    @Volatile var responseBody = ""
    @Volatile var lastPath: String? = null
    @Volatile var lastQuery: Map<String, String?> = emptyMap()

    override fun dispatch(request: RecordedRequest): MockResponse {
        request.requestUrl?.let { url ->
            lastPath = url.encodedPath
            lastQuery = url.queryParameterNames.associateWith { url.queryParameter(it) }
        }
        return MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json").setBody(responseBody)
    }
}
