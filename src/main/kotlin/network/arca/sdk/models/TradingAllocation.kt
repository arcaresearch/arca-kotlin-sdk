package network.arca.sdk.models

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Arca capital allocation, independent of venue liquidation and isolation. */
@Serializable
public enum class LeveragePreferenceMode {
    @SerialName("venue-default") VENUE_DEFAULT,
    @SerialName("fixed") FIXED,
}

@Serializable
public data class TradingLeveragePreference(
    public val mode: LeveragePreferenceMode,
    public val leverage: Int? = null,
)

@Serializable
public data class PositionAllocation(
    public val market: String,
    public val preference: TradingLeveragePreference,
    public val effectiveLeverage: Int,
    public val venueInitialMargin: String,
    public val allocatedMargin: String,
    public val extraMargin: String,
    public val reservedMargin: String,
    public val reservedExtra: String,
)

@Serializable
public data class TradingAllocationProjection(
    public val revision: String,
    public val positions: Map<String, PositionAllocation>,
    public val venueInitialMargin: String,
    public val positionAllocatedMargin: String,
    public val allocatedMargin: String,
    public val extraMargin: String,
    public val pendingMargin: String,
    public val pendingCosts: String,
    public val reservedExtra: String,
    public val availableToTrade: String,
)

@Serializable
public data class TradingAllocationState(
    public val asOf: String? = null,
    public val validUntil: String? = null,
    public val revision: String,
    /** Includes preferences for flat markets. */
    public val preferences: Map<String, TradingLeveragePreference>,
    public val projection: TradingAllocationProjection? = null,
    /** A missing projection means unavailable facts, not zero collateral. */
    public val projectionUnavailable: Boolean,
)

@Serializable
public data class TradingLeverageSelection(
    public val mode: LeveragePreferenceMode? = null,
    public val leverage: Int? = null,
)

@Serializable
public data class TradingAllocationRead(
    public val enabled: Boolean,
    public val inputId: String,
    public val allocation: TradingAllocationState,
    public val unavailableReason: String? = null,
)

/** Estimate only. orderType is lowercase "market" or "limit". */
@Serializable
public data class TradingAllocationQuoteRequest(
    public val market: String,
    public val side: OrderSide,
    public val orderType: String,
    public val price: String? = null,
    public val size: String? = null,
    public val slippageBps: Int? = null,
    public val reduceOnly: Boolean = false,
    public val selection: TradingLeverageSelection = TradingLeverageSelection(),
)

@Serializable
public data class TradingAllocationMaximum(
    public val revision: String,
    public val maxSize: String,
    public val maxNotional: String,
    public val projection: TradingAllocationProjection? = null,
)

@Serializable
public data class TradingAllocationQuote(
    public val inputId: String,
    public val market: String,
    public val referencePrice: String,
    public val limitPrice: String,
    public val allocation: TradingAllocationState,
    public val maximum: TradingAllocationMaximum,
    public val affordable: Boolean? = null,
    public val orderProjection: TradingAllocationProjection? = null,
)
