package network.arca.sdk

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.Market
import network.arca.sdk.models.OrderBreakdown
import network.arca.sdk.models.OrderBreakdownOptions
import okhttp3.OkHttpClient
import java.util.Base64
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * The Arca SDK client for Android / JVM apps.
 *
 * Authenticates with a scoped JWT token (minted by the app's backend). All
 * network methods are `suspend` functions and use Kotlin coroutines.
 *
 * **Static token (manual refresh):**
 * ```kotlin
 * val arca = Arca(token = scopedJwt)
 * ```
 *
 * **With token provider (automatic refresh):**
 * ```kotlin
 * val arca = Arca(token = scopedJwt, tokenProvider = { fetchFreshToken() })
 * ```
 *
 * **Provider-only (no initial token):**
 * ```kotlin
 * val arca = Arca.withTokenProvider { fetchFreshToken() }
 * ```
 */
public class Arca private constructor(
    internal val realmId: String,
    public val candleCdnBaseUrl: String?,
    public val client: ArcaClient,
    public val ws: WebSocketManager,
    public val tokenManager: TokenManager,
    public val historyCache: HistoryCache,
    /**
     * Diagnostic logger. Exposed so builders can log their own records alongside
     * SDK records, and so SDK extensions can reach the logger.
     */
    public val log: ArcaLogger,
    /** Shared OkHttp client, reused by the candle-CDN fetcher. */
    internal val httpClient: OkHttpClient,
) {
    /** Shared coroutine scope for SDK-managed background work (handles, watches). */
    internal val scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    private val autoTracking = AtomicReference(AutoTrackingState())

    private val metaMutex = Mutex()

    @Volatile
    private var metaAssets: Map<String, Market>? = null
    private var metaInflight: Deferred<Map<String, Market>>? = null

    private data class AutoTrackingState(
        val operations: OperationWatchStream? = null,
        val balances: BalanceWatchStream? = null,
        val exchange: ExchangeStateWatchStream? = null,
    )

    /** The resolved realm ID for this SDK instance. */
    public val realm: String get() = realmId

    // MARK: - Token management

    /**
     * Update the bearer token after a refresh. Updates both the HTTP client and
     * the WebSocket manager (which reconnects immediately if disconnected).
     */
    public suspend fun updateToken(newToken: String) {
        client.updateToken(newToken)
        ws.updateToken(newToken)
        tokenManager.scheduleProactiveRefresh(newToken) { token ->
            client.updateToken(token)
            ws.updateToken(token)
        }
    }

    /** Force the WebSocket to disconnect and immediately reconnect. */
    public fun reconnect() {
        ws.reconnect()
    }

    /**
     * Register a listener for unrecoverable authentication errors. Returns an ID
     * to pass to [removeAuthErrorHandler].
     */
    public fun onAuthError(handler: (Throwable) -> Unit): String = tokenManager.onAuthError(handler)

    /** Remove a previously registered auth error handler. */
    public fun removeAuthErrorHandler(id: String) {
        tokenManager.removeAuthErrorHandler(id)
    }

    /**
     * Clear the in-memory cache of historical data responses (equity history,
     * P&L history, candles).
     */
    public fun clearHistoryCache() {
        historyCache.clear()
    }

    /** Tear down all background work. Call when the SDK instance is no longer needed. */
    public fun close() {
        ws.shutdown()
        tokenManager.shutdown()
        scope.cancel()
    }

    // MARK: - Auto-tracking

    /**
     * Enable optimistic tracking: mutation methods immediately inject submitted
     * operations into the provided watch streams, giving instant UI feedback
     * before server-side events arrive.
     */
    public fun enableAutoTracking(
        operations: OperationWatchStream? = null,
        balances: BalanceWatchStream? = null,
        exchange: ExchangeStateWatchStream? = null,
    ) {
        operations?.trackingScope = scope
        autoTracking.set(AutoTrackingState(operations, balances, exchange))
    }

    /** Disable optimistic tracking and release stream references. */
    public fun disableAutoTracking() {
        autoTracking.set(AutoTrackingState())
    }

    // MARK: - Operation handle factory

    /**
     * Create an [OperationHandle] that starts the HTTP call eagerly and wires up
     * WebSocket-based settlement waiting.
     */
    internal fun <T : OperationResponse> operationHandle(submit: suspend () -> T): OperationHandle<T> {
        val handle = OperationHandle(scope, submit) { operationId -> waitForSettlement(operationId) }
        val opStream = autoTracking.get().operations
        if (opStream != null) {
            opStream.trackingScope = scope
            opStream.trackSubmission(handle)
        }
        return handle
    }

    // MARK: - Market meta cache

    /**
     * Ensure market metadata is loaded, coalescing concurrent requests. Returns
     * the cached map keyed by canonical market ID (`Market.name`).
     */
    internal suspend fun ensureMetaLoaded(forceRefresh: Boolean = false): Map<String, Market> {
        if (!forceRefresh) {
            metaAssets?.let { return it }
        }

        val task: Deferred<Map<String, Market>>? = metaMutex.withLock {
            if (forceRefresh) {
                metaInflight?.cancel()
                metaAssets = null
                metaInflight = null
            }
            val cached = metaAssets
            if (cached != null && !forceRefresh) {
                null
            } else {
                metaInflight ?: scope.async {
                    try {
                        val response = getMarketMeta()
                        val map = LinkedHashMap<String, Market>(response.universe.size)
                        for (asset in response.universe) map[asset.name] = asset
                        metaMutex.withLock {
                            metaAssets = map
                            metaInflight = null
                        }
                        map
                    } catch (e: Throwable) {
                        metaMutex.withLock { metaInflight = null }
                        throw e
                    }
                }.also { metaInflight = it }
            }
        }

        return task?.await() ?: (metaAssets ?: emptyMap())
    }

    public companion object {
        /** Default Arca API base URL. */
        public const val DEFAULT_BASE_URL: String = "https://api.arcaos.io"

        /** Default candle CDN base URL. */
        public const val DEFAULT_CDN_BASE_URL: String = "https://data.arcaos.io"

        /**
         * Initialize the SDK from a scoped JWT token, with optional automatic
         * refresh via [tokenProvider].
         */
        public operator fun invoke(
            token: String,
            baseUrl: String = DEFAULT_BASE_URL,
            realmId: String? = null,
            tokenProvider: TokenProvider? = null,
            cache: CacheConfig = CacheConfig(),
            candleCdnBaseUrl: String? = DEFAULT_CDN_BASE_URL,
            logLevel: ArcaLogLevel = ArcaLogLevel.WARNING,
            logHandler: ArcaLogHandler? = null,
            lifecycleBridge: AppLifecycleBridge? = null,
            httpClient: OkHttpClient? = null,
        ): Arca {
            val resolved = realmId ?: extractRealmId(token)
            val logger = ArcaLogger(logLevel, logHandler)
            val tokenManager = TokenManager(tokenProvider)
            tokenManager.attachLogger(logger)

            val baseClient = httpClient ?: OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(60, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build()
            // WebSocket client shares the connection pool / dispatcher but disables
            // the read timeout so the long-lived socket isn't dropped.
            val wsClient = baseClient.newBuilder()
                .readTimeout(0, TimeUnit.MILLISECONDS)
                .build()

            val onUnauthorized: (suspend () -> String)? =
                if (tokenProvider != null) ({ tokenManager.refreshToken() }) else null
            val wsGetToken: (suspend () -> String)? =
                if (tokenProvider != null) ({ tokenManager.refreshToken() }) else null
            val onAuthError: (Throwable) -> Unit = { error -> tokenManager.emitAuthError(error) }

            val client = ArcaClient(
                token = token,
                baseUrl = baseUrl,
                httpClient = baseClient,
                onUnauthorized = onUnauthorized,
                onAuthError = onAuthError,
                logger = logger,
            )

            val ws = WebSocketManager(
                baseUrl = baseUrl,
                token = token,
                realmId = resolved,
                httpClient = wsClient,
                getToken = wsGetToken,
                log = logger,
                lifecycleBridge = lifecycleBridge,
            )

            val arca = Arca(
                realmId = resolved,
                candleCdnBaseUrl = if (candleCdnBaseUrl.isNullOrEmpty()) null else candleCdnBaseUrl,
                client = client,
                ws = ws,
                tokenManager = tokenManager,
                historyCache = HistoryCache(cache),
                log = logger,
                httpClient = baseClient,
            )

            if (tokenProvider != null) {
                tokenManager.scheduleProactiveRefresh(token) { newToken ->
                    client.updateToken(newToken)
                    ws.updateToken(newToken)
                }
            }

            return arca
        }

        /**
         * Create an Arca instance using only a token provider (no initial token).
         * The provider is called immediately to obtain the first token.
         */
        public suspend fun withTokenProvider(
            tokenProvider: TokenProvider,
            baseUrl: String = DEFAULT_BASE_URL,
            realmId: String? = null,
            cache: CacheConfig = CacheConfig(),
            candleCdnBaseUrl: String? = DEFAULT_CDN_BASE_URL,
            logLevel: ArcaLogLevel = ArcaLogLevel.WARNING,
            logHandler: ArcaLogHandler? = null,
            lifecycleBridge: AppLifecycleBridge? = null,
            httpClient: OkHttpClient? = null,
        ): Arca {
            val token = tokenProvider()
            return invoke(
                token = token,
                baseUrl = baseUrl,
                realmId = realmId,
                tokenProvider = tokenProvider,
                cache = cache,
                candleCdnBaseUrl = candleCdnBaseUrl,
                logLevel = logLevel,
                logHandler = logHandler,
                lifecycleBridge = lifecycleBridge,
                httpClient = httpClient,
            )
        }

        /**
         * Pure calculator that converts between spend (gross), notional (net),
         * and token representations of an order. No network call.
         */
        public fun orderBreakdown(options: OrderBreakdownOptions): OrderBreakdown =
            computeOrderBreakdown(options)

        private fun extractRealmId(token: String): String {
            val parts = token.split(".")
            if (parts.size != 3) {
                throw ArcaException.Validation("Invalid JWT format — expected 3 parts", null)
            }
            val realmId = runCatching {
                val padded = parts[1].let { it + "=".repeat((4 - it.length % 4) % 4) }
                val bytes = Base64.getUrlDecoder().decode(padded)
                arcaJson.parseToJsonElement(String(bytes)).jsonObject["realmId"]?.jsonPrimitive?.contentOrNull
            }.getOrNull()
            return realmId ?: throw ArcaException.Validation(
                "Token does not contain a realmId claim. Pass realmId explicitly.",
                null,
            )
        }
    }
}
