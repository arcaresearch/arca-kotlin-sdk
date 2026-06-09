package network.arca.sdk

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.Serializable
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.OrderSide
import network.arca.sdk.models.Twap
import network.arca.sdk.models.TwapLimits
import network.arca.sdk.models.TwapOperationResponse
import network.arca.sdk.models.TypedEvent

// MARK: - TWAP Methods

/**
 * Start a TWAP (Time-Weighted Average Price) order that executes a total size
 * over a duration by placing market orders at regular intervals.
 *
 * @param path Operation path (idempotency key).
 * @param exchangeId Exchange Arca object ID.
 * @param market Canonical market identifier (e.g. `"hl:0:BTC"`).
 * @param side [OrderSide.BUY] or [OrderSide.SELL].
 * @param totalSize Total size to execute over the duration.
 * @param durationMinutes Duration in minutes (1 to 43200).
 * @param intervalSeconds Interval between slices in seconds (10 to 3600, default 30).
 * @param randomize Add timing jitter to slice placement.
 * @param reduceOnly Reduce-only mode.
 * @param leverage Leverage multiplier.
 * @param slippageBps Max slippage in basis points (10 to 1000, default 300).
 */
public fun Arca.placeTwap(
    path: String,
    exchangeId: String,
    market: String,
    side: OrderSide,
    totalSize: String,
    durationMinutes: Int,
    intervalSeconds: Int? = null,
    randomize: Boolean = false,
    reduceOnly: Boolean = false,
    leverage: Int? = null,
    slippageBps: Int? = null,
): OperationHandle<TwapOperationResponse> =
    operationHandle {
        val body = arcaJson.encodeToJsonElement(
            PlaceTwapRequest.serializer(),
            PlaceTwapRequest(
                realmId = realm,
                path = path,
                market = market,
                side = side.wire,
                totalSize = totalSize,
                durationMinutes = durationMinutes,
                intervalSeconds = intervalSeconds,
                randomize = randomize,
                reduceOnly = reduceOnly,
                leverage = leverage,
                slippageBps = slippageBps,
            ),
        )
        client.post<TwapOperationResponse>("/objects/$exchangeId/exchange/twap", body = body)
    }

/**
 * Cancel an active TWAP.
 *
 * @param exchangeId Exchange Arca object ID.
 * @param operationId The parent operation ID of the TWAP.
 */
public fun Arca.cancelTwap(
    exchangeId: String,
    operationId: String,
): OperationHandle<TwapOperationResponse> =
    operationHandle {
        client.delete<TwapOperationResponse>(
            "/objects/$exchangeId/exchange/twap/$operationId",
            query = mapOf("realmId" to realm),
        )
    }

/**
 * Get TWAP status and progress by its parent operation ID.
 *
 * @param exchangeId Exchange Arca object ID.
 * @param operationId The parent operation ID of the TWAP.
 */
public suspend fun Arca.getTwap(exchangeId: String, operationId: String): TwapOperationResponse =
    client.get("/objects/$exchangeId/exchange/twap/$operationId", query = mapOf("realmId" to realm))

/**
 * List TWAPs for an exchange object.
 *
 * @param exchangeId Exchange Arca object ID.
 * @param activeOnly If true, only returns active TWAPs.
 */
public suspend fun Arca.listTwaps(exchangeId: String, activeOnly: Boolean = false): List<Twap> {
    val query = mutableMapOf("realmId" to realm)
    if (activeOnly) query["active"] = "true"
    return client.get("/objects/$exchangeId/exchange/twaps", query = query)
}

/**
 * Get TWAP limits + recommendation curve from the server. The response is static
 * for the process lifetime; the SDK caches it after the first call.
 *
 * Use `getTwapLimits().recommendation.buckets` directly for custom pickers, or
 * call [recommendedIntervalSeconds] for the one-shot helper.
 */
public suspend fun Arca.getTwapLimits(): TwapLimits =
    TwapLimitsCache.getOrFetch { client.get("/twap/limits") }

/**
 * Returns the recommended `intervalSeconds` for a given TWAP duration, picked
 * from the server's recommendation curve. Use this to populate a default in your
 * TWAP entry UI.
 */
public suspend fun Arca.recommendedIntervalSeconds(durationMinutes: Int): Int {
    val response = getTwapLimits()
    for (bucket in response.recommendation.buckets) {
        if (durationMinutes <= bucket.maxDurationMinutes) {
            return bucket.recommendedIntervalSeconds
        }
    }
    return response.limits.defaultIntervalSeconds
}

/**
 * Watch a single TWAP by its parent operation ID. The returned [Flow] emits the
 * latest server-side [Twap] snapshot on every TWAP event targeting this operation
 * (`twap.started`, `twap.progress`, `twap.completed`, `twap.cancelled`,
 * `twap.failed`).
 *
 * The first element is the result of an eager REST fetch so the caller can render
 * initial state without waiting for an event. Subsequent elements are pushed by
 * the WebSocket.
 */
public fun Arca.watchTwap(exchangeId: String, operationId: String): Flow<Twap> = flow {
    runCatching { getTwap(exchangeId, operationId).twap }.getOrNull()?.let { emit(it) }
    ws.twapEvents().collect { event ->
        val twap = when (event) {
            is TypedEvent.TwapStarted -> event.twap
            is TypedEvent.TwapProgress -> event.twap
            is TypedEvent.TwapCompleted -> event.twap
            is TypedEvent.TwapCancelled -> event.twap
            is TypedEvent.TwapFailed -> event.twap
            else -> null
        }
        if (twap != null && twap.operationId == operationId) {
            emit(twap)
        }
    }
}

/**
 * Process-lifetime cache for the [TwapLimits] response. A [Mutex] coalesces
 * concurrent [Arca.getTwapLimits] calls into a single in-flight network request.
 */
private object TwapLimitsCache {
    private val mutex = Mutex()

    @Volatile
    private var cached: TwapLimits? = null

    suspend fun getOrFetch(fetch: suspend () -> TwapLimits): TwapLimits {
        cached?.let { return it }
        return mutex.withLock {
            cached ?: fetch().also { cached = it }
        }
    }
}

// MARK: - Private Request Types

@Serializable
private data class PlaceTwapRequest(
    val realmId: String,
    val path: String,
    val market: String,
    val side: String,
    val totalSize: String,
    val durationMinutes: Int,
    val intervalSeconds: Int? = null,
    val randomize: Boolean,
    val reduceOnly: Boolean,
    val leverage: Int? = null,
    val slippageBps: Int? = null,
)
