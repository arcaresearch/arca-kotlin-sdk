package network.arca.sdk.models

import kotlinx.serialization.Serializable
import network.arca.sdk.OperationResponse

/**
 * A TWAP order. It is scheduled as [expectedSliceCount] slots, one every
 * [intervalSeconds] from [startTime]. In each slot at most one slice is sent:
 * an IOC market order for an even share of what is left over the remaining
 * slots, never more than three planned slices and never more than
 * `totalSize − executedSize`. A slot passes without a slice while the
 * previous slice is still in flight, or when its share is below the venue's
 * minimum order. The TWAP completes when [executedSize] reaches [totalSize],
 * when no slot is left, or at [endTime]; it fails after five failed slices in
 * a row.
 */
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
    /** Summed fill size of settled slices, partial fills included. */
    public val executedSize: String,
    /** Summed fill size × average fill price (USD) of settled slices. */
    public val executedNotional: String,
    /** Slices sent so far, including one still in flight; never exceeds [expectedSliceCount]. */
    public val sliceCount: Int,
    /** Number of slots: `max(1, durationSeconds / intervalSeconds)`. Stable for the TWAP's lifetime. */
    public val expectedSliceCount: Int,
    /** Settled slices that executed a nonzero size, partial fills included. */
    public val filledSlices: Int,
    /**
     * Settled slices that executed nothing (rejected, failed, or an IOC that found no
     * liquidity within the slippage cap). `sliceCount − filledSlices − failedSlices` is 1
     * while a slice is in flight and 0 otherwise.
     */
    public val failedSlices: Int,
    public val intervalSeconds: Int,
    public val durationMinutes: Int,
    public val startTime: String,
    public val endTime: String? = null,
    public val status: TwapStatus,
    /**
     * Terminal-state discriminator: `user`, `liquidated`, `consecutive_failures`,
     * `progress_unknown` (started before fills were counted into [executedSize], so
     * stopped rather than risk trading past its total).
     */
    public val cancelReason: String? = null,
    /** Human-readable failure detail when the TWAP terminates as `failed`. */
    public val failureReason: String? = null,
    /** Mid-price snapshot at creation. Null when mid was unavailable. */
    public val targetPrice: String? = null,
    public val reduceOnly: Boolean,
    public val leverage: Int? = null,
    public val slippageBps: Int,
    public val randomize: Boolean,
    /** Failed slices since the last filled one; the TWAP fails at 5. */
    public val consecutiveFailures: Int,
    /**
     * When the next slot opens and the next slice is due. Null once the TWAP is no longer
     * active. At or after [endTime] it means no slot is left and the TWAP completes once
     * its in-flight slice settles.
     */
    public val nextSliceAt: String? = null,
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
