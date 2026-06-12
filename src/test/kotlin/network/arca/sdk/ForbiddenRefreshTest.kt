package network.arca.sdk

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger

/**
 * Pins the 403 → token-provider refresh contract.
 *
 * A cached token can be valid (not expired) but scoped to a different
 * identity than the provider would now mint for — e.g. the app switched
 * signed-in users. The server rejects such requests with 403 `FORBIDDEN` /
 * `REALM_SCOPE_MISMATCH`, NOT 401, so the client must treat a 403 as a
 * refresh trigger when a provider is configured. Without a provider a 403
 * is a plain permission denial: no refresh, no onAuthError.
 */
class ForbiddenRefreshTest {

    @Serializable
    data class Probe(val ok: Boolean)

    private lateinit var server: MockWebServer

    @BeforeEach
    fun setUp() {
        server = MockWebServer()
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    private fun successResponse(): MockResponse = MockResponse()
        .setHeader("Content-Type", "application/json")
        .setBody("""{"success":true,"data":{"ok":true}}""")

    private fun errorResponse(status: Int, code: String, message: String): MockResponse = MockResponse()
        .setResponseCode(status)
        .setHeader("Content-Type", "application/json")
        .setBody("""{"success":false,"error":{"code":"$code","message":"$message"}}""")

    @Test
    fun forbiddenTriggersRefreshAndRetry() = runBlocking {
        server.enqueue(errorResponse(403, "FORBIDDEN", "Access denied"))
        server.enqueue(successResponse())

        var trigger: AuthRefreshTrigger? = null
        val client = ArcaClient(
            token = "stale-identity-token",
            baseUrl = server.url("/").toString().trimEnd('/'),
            httpClient = OkHttpClient(),
            onUnauthorized = { t ->
                trigger = t
                "fresh-identity-token"
            },
        )

        val probe: Probe = client.get("/probe")
        assertTrue(probe.ok)
        assertEquals(AuthRefreshTrigger.FORBIDDEN, trigger)

        server.takeRequest() // first (403) request
        val retried = server.takeRequest()
        assertEquals("Bearer fresh-identity-token", retried.getHeader("Authorization"))
    }

    @Test
    fun realmScopeMismatchTriggersRefreshAndMapsToForbidden() = runBlocking {
        server.enqueue(errorResponse(403, "REALM_SCOPE_MISMATCH", "Token is scoped to a different realm"))
        server.enqueue(successResponse())

        val refreshCount = AtomicInteger(0)
        val client = ArcaClient(
            token = "stale",
            baseUrl = server.url("/").toString().trimEnd('/'),
            httpClient = OkHttpClient(),
            onUnauthorized = { _ ->
                refreshCount.incrementAndGet()
                "fresh"
            },
        )

        val probe: Probe = client.get("/probe")
        assertTrue(probe.ok)
        assertEquals(1, refreshCount.get())

        // Mapping check: the code is preserved on the exception type.
        val mapped = mapApiError("REALM_SCOPE_MISMATCH", "mismatch", null)
        assertTrue(mapped is ArcaException.Forbidden)
        assertEquals("REALM_SCOPE_MISMATCH", (mapped as ArcaException.Forbidden).code)
    }

    @Test
    fun unauthorizedPassesUnauthorizedTrigger() = runBlocking {
        server.enqueue(errorResponse(401, "UNAUTHORIZED", "expired"))
        server.enqueue(successResponse())

        var trigger: AuthRefreshTrigger? = null
        val client = ArcaClient(
            token = "expired-token",
            baseUrl = server.url("/").toString().trimEnd('/'),
            httpClient = OkHttpClient(),
            onUnauthorized = { t ->
                trigger = t
                "fresh"
            },
        )

        val probe: Probe = client.get("/probe")
        assertTrue(probe.ok)
        assertEquals(AuthRefreshTrigger.UNAUTHORIZED, trigger)
    }

    @Test
    fun forbiddenWithoutProviderThrowsWithoutAuthError() = runBlocking {
        server.enqueue(errorResponse(403, "FORBIDDEN", "Access denied"))

        var authError: Throwable? = null
        val client = ArcaClient(
            token = "token",
            baseUrl = server.url("/").toString().trimEnd('/'),
            httpClient = OkHttpClient(),
            onAuthError = { authError = it },
        )

        val thrown = runCatching { client.get<Probe>("/probe") }.exceptionOrNull()
        assertTrue(thrown is ArcaException.Forbidden, "expected Forbidden, got $thrown")
        // A plain permission denial must not look like session expiry.
        assertNull(authError)
    }

    @Test
    fun stillForbiddenAfterRefreshEmitsAuthError() = runBlocking {
        server.enqueue(errorResponse(403, "FORBIDDEN", "Access denied"))
        server.enqueue(errorResponse(403, "FORBIDDEN", "still denied"))

        var authError: Throwable? = null
        val client = ArcaClient(
            token = "stale",
            baseUrl = server.url("/").toString().trimEnd('/'),
            httpClient = OkHttpClient(),
            onUnauthorized = { _ -> "still-wrong-identity" },
            onAuthError = { authError = it },
        )

        val thrown = runCatching { client.get<Probe>("/probe") }.exceptionOrNull()
        assertTrue(thrown is ArcaException.Forbidden, "expected Forbidden, got $thrown")
        assertTrue(authError is ArcaException.Forbidden, "expected onAuthError with Forbidden, got $authError")
    }
}
