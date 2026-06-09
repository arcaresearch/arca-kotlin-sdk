package network.arca.sdk

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Base64
import java.util.Collections

/**
 * Verifies that `watchFills`, `watchFunding`, and `watchExchangeState` propagate
 * errors from `getObjectDetail` instead of silently falling back to watching "/".
 */
class WatchErrorPropagationTest {

    private lateinit var server: MockWebServer

    @BeforeEach
    fun setUp() {
        server = MockWebServer()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val path = request.path ?: ""
                if (path.contains("/objects/")) {
                    return MockResponse()
                        .setResponseCode(404)
                        .setHeader("Content-Type", "application/json")
                        .setBody("""{"success":false,"error":{"code":"OBJECT_NOT_FOUND","message":"Object not found"}}""")
                }
                // Anything else (incl. the WebSocket upgrade) fails the connection harmlessly.
                return MockResponse().setResponseCode(404)
            }
        }
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    @Test
    fun watchFillsThrowsWhenGetObjectDetailFails() = runBlocking {
        val arca = makeArca()
        val thrown = runCatching { arca.watchFills(objectId = "nonexistent") }.exceptionOrNull()
        assertTrue(thrown is ArcaException.NotFound, "Expected NotFound, got $thrown")
    }

    @Test
    fun watchFundingThrowsWhenGetObjectDetailFails() = runBlocking {
        val arca = makeArca()
        val thrown = runCatching { arca.watchFunding(objectId = "nonexistent") }.exceptionOrNull()
        assertTrue(thrown is ArcaException.NotFound, "Expected NotFound, got $thrown")
    }

    @Test
    fun watchExchangeStateThrowsWhenGetObjectDetailFails() = runBlocking {
        val arca = makeArca()
        val thrown = runCatching { arca.watchExchangeState(objectId = "nonexistent") }.exceptionOrNull()
        assertTrue(thrown is ArcaException.NotFound, "Expected NotFound, got $thrown")
    }

    @Test
    fun restFailureSurfacesViaLogHandler() = runBlocking {
        val handler = WatchCapturingLogHandler()
        val arca = makeArca(logLevel = ArcaLogLevel.DEBUG, logHandler = handler)
        runCatching { arca.watchFills(objectId = "nonexistent") }

        val isRelevant = { r: ArcaLogRecord ->
            r.level >= ArcaLogLevel.WARNING && (r.category == "network" || r.category == "watch")
        }
        val deadline = System.currentTimeMillis() + 2000
        while (handler.records.none(isRelevant) && System.currentTimeMillis() < deadline) {
            delay(10)
        }
        assertTrue(
            handler.records.any(isRelevant),
            "Expected a warning on network/watch; got ${handler.records.map { Triple(it.level, it.category, it.message) }}",
        )
    }

    // MARK: - Helpers

    private fun makeArca(logLevel: ArcaLogLevel = ArcaLogLevel.WARNING, logHandler: ArcaLogHandler? = null): Arca =
        Arca(
            token = fakeJwt(),
            baseUrl = server.url("/").toString().trimEnd('/'),
            logLevel = logLevel,
            logHandler = logHandler,
        )

    private fun fakeJwt(): String {
        val enc = Base64.getUrlEncoder().withoutPadding()
        val header = enc.encodeToString("""{"alg":"HS256","typ":"JWT"}""".toByteArray())
        val payload = enc.encodeToString("""{"realmId":"rlm_test","sub":"usr_test"}""".toByteArray())
        return "$header.$payload.fakesig"
    }
}

private class WatchCapturingLogHandler : ArcaLogHandler {
    private val store = Collections.synchronizedList(mutableListOf<ArcaLogRecord>())
    val records: List<ArcaLogRecord> get() = synchronized(store) { store.toList() }
    override fun handle(record: ArcaLogRecord) {
        store.add(record)
    }
}
