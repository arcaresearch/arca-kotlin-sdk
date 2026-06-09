package network.arca.sdk.models

import kotlinx.serialization.Serializable
import network.arca.sdk.OperationResponse

@Serializable
public data class Twap(
    public val twapId: String,
    public val realmId: String,
    public val operationId: String,
    public val exchangeObjectId: String,
    public val exchangeObjectPath: String,
    public val simAccountId: String,
    public val type: TwapType,
    public val market: String,
    public val side: String,
    public val totalSize: String? = null,
    public val executedSize: String,
    public val executedNotional: String,
    /** Running counter of slices dispatched so far (starts at 0). */
    public val sliceCount: Int,
    /** Planned total slice count, stable for the TWAP's lifetime. */
    public val expectedSliceCount: Int,
    public val filledSlices: Int,
    public val failedSlices: Int,
    public val intervalSeconds: Int,
    public val durationMinutes: Int,
    public val startTime: String,
    public val endTime: String? = null,
    public val status: TwapStatus,
    /** Terminal-state discriminator: `user`, `liquidated`, `consecutive_failures`. */
    public val cancelReason: String? = null,
    /** Human-readable failure detail when the TWAP terminates as `failed`. */
    public val failureReason: String? = null,
    /** Mid-price snapshot at creation. Null when mid was unavailable. */
    public val targetPrice: String? = null,
    public val reduceOnly: Boolean,
    public val leverage: Int? = null,
    public val slippageBps: Int,
    public val randomize: Boolean,
    public val consecutiveFailures: Int,
    public val createdAt: String,
    public val updatedAt: String,
)

@Serializable
public data class TwapOperationResponse(
    public val twap: Twap,
    override val operation: Operation,
) : OperationResponse {
    override fun withOperation(operation: Operation): TwapOperationResponse = copy(operation = operation)
}

/**
 * Universal TWAP constraints plus a duration-keyed recommendation curve.
 * SDKs cache the response for the process lifetime.
 */
@Serializable
public data class TwapLimits(
    public val limits: TwapLimitsConfig,
    public val recommendation: TwapRecommendationCurve,
)

@Serializable
public data class TwapLimitsConfig(
    public val minTotalSize: String,
    public val maxDurationMinutes: Int,
    public val minIntervalSeconds: Int,
    public val maxIntervalSeconds: Int,
    public val minSlippageBps: Int,
    public val maxSlippageBps: Int,
    public val defaultIntervalSeconds: Int,
    public val defaultSlippageBps: Int,
    public val maxConcurrentPerObject: Int,
)

@Serializable
public data class TwapRecommendationCurve(
    /** Sorted by `maxDurationMinutes` ascending. */
    public val buckets: List<TwapRecommendationBucket>,
)

@Serializable
public data class TwapRecommendationBucket(
    public val maxDurationMinutes: Int,
    public val recommendedIntervalSeconds: Int,
)
