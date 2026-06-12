package network.arca.sdk

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.serializer
import network.arca.sdk.internal.ApiResponse
import network.arca.sdk.internal.arcaJson
import okhttp3.Call
import okhttp3.Callback
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import java.io.IOException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Why the client is asking for a fresh credential:
 * - [UNAUTHORIZED] — HTTP 401: the credential is invalid or expired.
 * - [FORBIDDEN] — HTTP 403 `FORBIDDEN` / `REALM_SCOPE_MISMATCH`: the
 *   credential is still valid but its scope no longer covers the request
 *   (e.g. the app switched signed-in users, so the provider would now mint
 *   a token for a different identity).
 */
public enum class AuthRefreshTrigger { UNAUTHORIZED, FORBIDDEN }

/**
 * Low-level HTTP client for the Arca API, built on OkHttp + coroutines.
 *
 * Handles bearer-token injection, the standard `{ success, data, error }`
 * envelope, automatic retries for transient `502/503/504` responses, and a
 * single auth-refresh retry (on `401`, and on `403` `FORBIDDEN` /
 * `REALM_SCOPE_MISMATCH`) that refreshes the token via [onUnauthorized].
 * All methods are `suspend` and cancellation-aware; concurrent calls run in
 * parallel.
 */
public class ArcaClient(
    token: String,
    baseUrl: String,
    private val httpClient: OkHttpClient,
    private val onUnauthorized: (suspend (AuthRefreshTrigger) -> String)? = null,
    private val onAuthError: ((Throwable) -> Unit)? = null,
    private val logger: ArcaLogger = ArcaLogger.disabled,
) {
    @Volatile
    private var currentToken: String = token

    private val apiBase: HttpUrl =
        baseUrl.trimEnd('/').toHttpUrl().newBuilder().addPathSegments("api/v1").build()

    /** Update the bearer token (e.g., after a token refresh). */
    public fun updateToken(newToken: String) {
        currentToken = newToken
    }

    // MARK: - Public HTTP methods

    public suspend inline fun <reified T> get(path: String, query: Map<String, String>? = null): T =
        requestTyped("GET", path, query, null, serializer())

    public suspend inline fun <reified T> post(path: String, body: JsonElement? = null): T =
        requestTyped("POST", path, null, body, serializer())

    public suspend inline fun <reified T> patch(
        path: String,
        query: Map<String, String>? = null,
        body: JsonElement? = null,
    ): T = requestTyped("PATCH", path, query, body, serializer())

    public suspend inline fun <reified T> delete(path: String, query: Map<String, String>? = null): T =
        requestTyped("DELETE", path, query, null, serializer())

    // MARK: - Auth retry wrapper

    @PublishedApi
    internal suspend fun <T> requestTyped(
        method: String,
        path: String,
        query: Map<String, String>?,
        body: JsonElement?,
        deserializer: KSerializer<T>,
    ): T {
        try {
            return requestWithRetry(method, path, query, body, deserializer)
        } catch (e: ArcaException.Unauthorized) {
            val refresh = onUnauthorized
            if (refresh == null) {
                onAuthError?.invoke(e)
                throw e
            }
            return refreshAndRetry(AuthRefreshTrigger.UNAUTHORIZED, refresh, method, path, query, body, deserializer)
        } catch (e: ArcaException.Forbidden) {
            // A 403 on a provider-backed client can mean the cached token is
            // valid but scoped to a different identity than the provider
            // would now mint for (e.g. the app switched signed-in users).
            // Refresh once and retry. Without a provider this is a plain
            // permission denial: rethrow without emitting onAuthError.
            val refresh = onUnauthorized ?: throw e
            return refreshAndRetry(AuthRefreshTrigger.FORBIDDEN, refresh, method, path, query, body, deserializer)
        }
    }

    private suspend fun <T> refreshAndRetry(
        trigger: AuthRefreshTrigger,
        refresh: suspend (AuthRefreshTrigger) -> String,
        method: String,
        path: String,
        query: Map<String, String>?,
        body: JsonElement?,
        deserializer: KSerializer<T>,
    ): T {
        val status = if (trigger == AuthRefreshTrigger.FORBIDDEN) "403" else "401"
        logger.notice("auth", metadata = mapOf("httpMethod" to method, "path" to path)) {
            "$status received, refreshing token and retrying"
        }
        try {
            currentToken = refresh(trigger)
            return requestWithRetry(method, path, query, body, deserializer)
        } catch (refreshError: Throwable) {
            logger.error("auth", refreshError, mapOf("httpMethod" to method, "path" to path)) {
                "token refresh failed after $status"
            }
            onAuthError?.invoke(refreshError)
            throw refreshError
        }
    }

    // MARK: - Retry logic

    private suspend fun <T> requestWithRetry(
        method: String,
        path: String,
        query: Map<String, String>?,
        body: JsonElement?,
        deserializer: KSerializer<T>,
    ): T {
        var lastError: Throwable? = null
        for (attempt in 0..MAX_RETRIES) {
            currentCoroutineContext().ensureActive()
            try {
                return requestOnce(method, path, query, body, deserializer)
            } catch (e: TransientHttpException) {
                lastError = e
                if (attempt == MAX_RETRIES) throw e
                logger.warning(
                    "network",
                    e,
                    mapOf(
                        "httpMethod" to method,
                        "path" to path,
                        "attempt" to (attempt + 1).toString(),
                        "maxRetries" to MAX_RETRIES.toString(),
                    ),
                ) { "transient failure, retrying" }
                delay(RETRY_DELAY_MS)
            }
        }
        throw lastError ?: IllegalStateException("retry loop exited without result")
    }

    // MARK: - Single request

    private suspend fun <T> requestOnce(
        method: String,
        path: String,
        query: Map<String, String>?,
        body: JsonElement?,
        deserializer: KSerializer<T>,
    ): T {
        val url = buildUrl(path, query)
        val requestBody = body?.let {
            it.toString().toRequestBody(JSON_MEDIA_TYPE)
        }
        val request = Request.Builder()
            .url(url)
            .header("Content-Type", "application/json")
            .header("Authorization", "Bearer $currentToken")
            .header(CLIENT_CAPABILITIES_HEADER, ADVERTISED_CAPABILITIES.joinToString(","))
            .method(method, requestBody)
            .build()

        logger.debug("network", metadata = mapOf("httpMethod" to method, "path" to path)) { "request" }

        val response: Response = try {
            withContext(Dispatchers.IO) { httpClient.newCall(request).await() }
        } catch (e: IOException) {
            logger.warning("network", e, mapOf("httpMethod" to method, "path" to path)) { "network failure" }
            throw ArcaException.Network(e)
        }

        val statusCode = response.code
        val bodyString = response.use { it.body?.string() ?: "" }

        if (statusCode in TRANSIENT_STATUSES) {
            throw TransientHttpException(statusCode)
        }

        logger.debug(
            "network",
            metadata = mapOf("httpMethod" to method, "path" to path, "statusCode" to statusCode.toString()),
        ) { "response" }

        try {
            return unwrap(bodyString, statusCode, method, path, deserializer)
        } catch (e: ArcaException.NonJsonResponse) {
            throw ArcaException.NonJsonResponse(e.statusCode, "[$method $path] ${e.body}")
        }
    }

    // MARK: - Response unwrapping

    private fun <T> unwrap(
        bodyString: String,
        statusCode: Int,
        method: String,
        path: String,
        deserializer: KSerializer<T>,
    ): T {
        val envelope: ApiResponse<T> = try {
            arcaJson.decodeFromString(ApiResponse.serializer(deserializer), bodyString)
        } catch (e: SerializationException) {
            throw nonJsonOrDecoding(bodyString, statusCode, method, path, e)
        } catch (e: IllegalArgumentException) {
            throw nonJsonOrDecoding(bodyString, statusCode, method, path, e)
        }

        if (!envelope.success || envelope.data == null) {
            if (statusCode == 401) {
                throw ArcaException.Unauthorized(
                    envelope.error?.message ?: "Invalid or expired authentication",
                    envelope.error?.errorId,
                )
            }
            val error = envelope.error
            if (error != null) {
                val mapped = mapApiError(error.code, error.message, error.errorId)
                logger.warning(
                    "network",
                    mapped,
                    mapOf(
                        "httpMethod" to method,
                        "path" to path,
                        "statusCode" to statusCode.toString(),
                        "code" to error.code,
                        "errorId" to (error.errorId ?: ""),
                    ),
                ) { "API error" }
                throw mapped
            }
            logger.warning(
                "network",
                metadata = mapOf("httpMethod" to method, "path" to path, "statusCode" to statusCode.toString()),
            ) { "request failed with no error envelope" }
            throw ArcaException.Unknown("UNKNOWN", "Request failed with status $statusCode", null)
        }

        return envelope.data
    }

    private fun nonJsonOrDecoding(
        bodyString: String,
        statusCode: Int,
        method: String,
        path: String,
        cause: Throwable,
    ): ArcaException {
        val trimmed = bodyString.trimStart()
        if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
            logger.error(
                "network",
                cause,
                mapOf("httpMethod" to method, "path" to path, "statusCode" to statusCode.toString()),
            ) { "response decode failed" }
            return ArcaException.Decoding(cause)
        }
        val preview = bodyString.take(200).ifEmpty { "<binary>" }
        logger.error(
            "network",
            metadata = mapOf(
                "httpMethod" to method,
                "path" to path,
                "statusCode" to statusCode.toString(),
                "bodyPreview" to preview,
            ),
        ) { "non-JSON response" }
        return ArcaException.NonJsonResponse(statusCode, preview)
    }

    private fun buildUrl(path: String, query: Map<String, String>?): HttpUrl =
        apiBase.newBuilder()
            .addPathSegments(path.trimStart('/'))
            .apply { query?.forEach { (k, v) -> addQueryParameter(k, v) } }
            .build()

    public companion object {
        /** HTTP header carrying the SDK's advertised capabilities (comma-separated). */
        public const val CLIENT_CAPABILITIES_HEADER: String = "X-Arca-Client-Capabilities"

        /** Capabilities this SDK advertises to the server over REST and the WS `auth` message. */
        public val ADVERTISED_CAPABILITIES: List<String> = listOf("server-authoritative-pricing")

        private val TRANSIENT_STATUSES: Set<Int> = setOf(502, 503, 504)
        private const val MAX_RETRIES = 2
        private const val RETRY_DELAY_MS = 1_000L
        private val JSON_MEDIA_TYPE = "application/json".toMediaType()
    }
}

/** Sentinel error for transient HTTP status codes (502/503/504). */
internal class TransientHttpException(val statusCode: Int) :
    Exception("Transient HTTP error: $statusCode")

/** Bridges an OkHttp [Call] to a cancellation-aware suspend function. */
internal suspend fun Call.await(): Response = suspendCancellableCoroutine { cont: CancellableContinuation<Response> ->
    cont.invokeOnCancellation { runCatching { cancel() } }
    enqueue(object : Callback {
        override fun onFailure(call: Call, e: IOException) {
            if (!cont.isCancelled) cont.resumeWithException(e)
        }

        override fun onResponse(call: Call, response: Response) {
            cont.resume(response)
        }
    })
}
