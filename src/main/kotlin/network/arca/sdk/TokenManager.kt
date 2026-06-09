package network.arca.sdk

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonPrimitive
import network.arca.sdk.internal.arcaJson
import java.util.Base64
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.max

/** Async function that returns a fresh scoped JWT token. */
public typealias TokenProvider = suspend () -> String

/**
 * Manages token lifecycle: proactive refresh, deduplication of concurrent
 * refreshes, and auth-error events. Mirrors the Swift `TokenManager` actor
 * using a [Mutex] for state and a background [CoroutineScope] for the proactive
 * refresh timer.
 */
public class TokenManager(private val provider: TokenProvider?) {
    private val mutex = Mutex()
    private var pendingRefresh: Deferred<String>? = null

    private val jobLock = Any()
    private var proactiveRefreshJob: Job? = null

    private val authErrorHandlers = ConcurrentHashMap<String, (Throwable) -> Unit>()

    @Volatile
    private var log: ArcaLogger = ArcaLogger.disabled

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    /** Attach the SDK's logger so the token manager can emit `auth` records. */
    public fun attachLogger(logger: ArcaLogger) {
        log = logger
    }

    /** Whether a token provider is configured. */
    public val hasProvider: Boolean
        get() = provider != null

    /**
     * Call the token provider, deduplicating concurrent calls. Throws if no
     * provider is configured or the provider fails.
     */
    public suspend fun refreshToken(): String {
        mutex.withLock { pendingRefresh }?.let { return it.await() }

        val p = provider ?: throw ArcaException.Unauthorized("No token provider configured", null)
        log.debug("auth") { "refreshing token via provider" }

        val task = mutex.withLock {
            pendingRefresh ?: scope.async { p() }.also { pendingRefresh = it }
        }

        return try {
            val token = task.await()
            mutex.withLock { if (pendingRefresh === task) pendingRefresh = null }
            token
        } catch (e: CancellationException) {
            mutex.withLock { if (pendingRefresh === task) pendingRefresh = null }
            throw e
        } catch (e: Throwable) {
            mutex.withLock { if (pendingRefresh === task) pendingRefresh = null }
            log.warning("auth", e) { "token provider failed" }
            throw e
        }
    }

    /**
     * Schedule a proactive refresh ~30s before the token's `exp` claim. When
     * the refresh succeeds, [onRefresh] is called with the new token.
     */
    public fun scheduleProactiveRefresh(token: String, onRefresh: suspend (String) -> Unit) {
        synchronized(jobLock) { proactiveRefreshJob?.cancel() }
        if (provider == null) return
        val exp = extractExpiry(token) ?: return

        val delaySeconds = max(0.0, exp - System.currentTimeMillis() / 1000.0 - REFRESH_BUFFER_SECONDS)

        val job = scope.launch {
            delay((delaySeconds * 1000).toLong())
            if (!isActive) return@launch
            try {
                val newToken = refreshToken()
                onRefresh(newToken)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                log.error("auth", e) { "proactive token refresh failed; emitting auth error" }
                emitAuthError(e)
            }
        }
        synchronized(jobLock) { proactiveRefreshJob = job }
    }

    /**
     * Register a handler for unrecoverable authentication errors. Returns an ID
     * to pass to [removeAuthErrorHandler].
     */
    public fun onAuthError(handler: (Throwable) -> Unit): String {
        val id = UUID.randomUUID().toString()
        authErrorHandlers[id] = handler
        return id
    }

    /** Remove a previously registered auth error handler. */
    public fun removeAuthErrorHandler(id: String) {
        authErrorHandlers.remove(id)
    }

    /** Emit an auth error to all registered handlers. */
    public fun emitAuthError(error: Throwable) {
        for (handler in authErrorHandlers.values) {
            handler(error)
        }
    }

    /** Cancel the background scope. Called by [Arca] on shutdown. */
    internal fun shutdown() {
        synchronized(jobLock) { proactiveRefreshJob?.cancel() }
        scope.cancel()
    }

    public companion object {
        private const val REFRESH_BUFFER_SECONDS = 30.0

        /** Extract the `exp` claim (seconds since epoch) from a JWT, or null. */
        internal fun extractExpiry(token: String): Double? {
            val parts = token.split(".")
            if (parts.size != 3) return null
            return try {
                val payloadBytes = Base64.getUrlDecoder().decode(padBase64Url(parts[1]))
                val json = arcaJson.parseToJsonElement(String(payloadBytes)) as? JsonObject ?: return null
                json["exp"]?.jsonPrimitive?.let { it.doubleOrNull ?: it.contentOrNull?.toDoubleOrNull() }
            } catch (_: Throwable) {
                null
            }
        }

        private fun padBase64Url(value: String): String {
            val rem = value.length % 4
            return if (rem == 0) value else value + "=".repeat(4 - rem)
        }
    }
}
