package network.arca.sdk

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.Base64
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class TokenProviderTest {

    private fun base64Url(s: String): String =
        Base64.getUrlEncoder().withoutPadding().encodeToString(s.toByteArray())

    private fun fakeJwt(extraClaims: Map<String, JsonElement> = emptyMap()): String {
        val header = base64Url("""{"alg":"HS256","typ":"JWT"}""")
        val claims = buildJsonObject {
            put("realmId", "rlm_test123")
            put("sub", "usr_abc")
            extraClaims.forEach { (k, v) -> put(k, v) }
        }
        return "$header.${base64Url(claims.toString())}.fakesig"
    }

    private fun fakeJwtWithExp(secondsFromNow: Double): String {
        val exp = System.currentTimeMillis() / 1000.0 + secondsFromNow
        return fakeJwt(mapOf("exp" to JsonPrimitive(exp)))
    }

    // MARK: - Construction

    @Test
    fun initWithTokenProvider() {
        val provider: TokenProvider = { fakeJwt() }
        val arca = Arca(token = fakeJwt(), tokenProvider = provider)
        assertEquals("rlm_test123", arca.realm)
    }

    @Test
    fun withTokenProviderFactory() = runBlocking {
        val jwt = fakeJwt()
        val arca = Arca.withTokenProvider({ jwt })
        assertEquals("rlm_test123", arca.realm)
    }

    @Test
    fun withTokenProviderFactoryCallsProvider() = runBlocking {
        val callCount = AtomicInteger(0)
        val jwt = fakeJwt()
        Arca.withTokenProvider({
            callCount.incrementAndGet()
            jwt
        })
        assertEquals(1, callCount.get())
    }

    // MARK: - TokenManager

    @Test
    fun tokenManagerRefreshDeduplication() = runBlocking {
        val callCount = AtomicInteger(0)
        val jwt = fakeJwt()
        val manager = TokenManager {
            callCount.incrementAndGet()
            delay(50)
            jwt
        }

        val t1 = async { manager.refreshToken() }
        val t2 = async { manager.refreshToken() }

        assertEquals(jwt, t1.await())
        assertEquals(jwt, t2.await())
        assertEquals(1, callCount.get())
    }

    @Test
    fun tokenManagerRefreshWithoutProvider() = runBlocking {
        val manager = TokenManager(null)
        val thrown = runCatching { manager.refreshToken() }.exceptionOrNull()
        assertTrue(thrown is ArcaException.Unauthorized, "Expected Unauthorized, got $thrown")
    }

    @Test
    fun tokenManagerHasProvider() {
        assertTrue(TokenManager { "token" }.hasProvider)
        assertFalse(TokenManager(null).hasProvider)
    }

    // MARK: - Auth error events

    @Test
    fun onAuthError() {
        val manager = TokenManager(null)
        var received: Throwable? = null
        val latch = CountDownLatch(1)

        manager.onAuthError {
            received = it
            latch.countDown()
        }
        manager.emitAuthError(ArcaException.Unauthorized("expired", null))

        assertTrue(latch.await(1, TimeUnit.SECONDS))
        assertNotNull(received)
    }

    @Test
    fun removeAuthErrorHandler() = runBlocking {
        val manager = TokenManager(null)
        var called = false

        val id = manager.onAuthError { called = true }
        manager.removeAuthErrorHandler(id)
        manager.emitAuthError(ArcaException.Unauthorized("expired", null))

        delay(50)
        assertFalse(called)
    }

    // MARK: - Proactive refresh

    @Test
    fun proactiveRefreshSchedules() = runBlocking {
        val freshJwt = fakeJwt(mapOf("sub" to JsonPrimitive("refreshed")))
        val manager = TokenManager { freshJwt }
        val almostExpired = fakeJwtWithExp(2.0) // expires in 2s, buffer is 30s, so fires immediately

        val result = CompletableDeferred<String>()
        manager.scheduleProactiveRefresh(almostExpired) { token -> result.complete(token) }

        val refreshed = withTimeout(3000) { result.await() }
        assertEquals(freshJwt, refreshed)
    }

    // MARK: - Arca onAuthError

    @Test
    fun arcaOnAuthError() {
        val arca = Arca(token = fakeJwt())
        var received: Throwable? = null
        val latch = CountDownLatch(1)

        val id = arca.onAuthError {
            received = it
            latch.countDown()
        }
        arca.tokenManager.emitAuthError(ArcaException.Unauthorized("test", null))

        assertTrue(latch.await(1, TimeUnit.SECONDS))
        assertNotNull(received)

        arca.removeAuthErrorHandler(id)
    }
}
