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
    public val revision: String,
    /** Includes preferences for flat markets. */
    public val preferences: Map<String, TradingLeveragePreference>,
    public val projection: TradingAllocationProjection? = null,
    /** A missing projection means unavailable facts, not zero collateral. */
    public val projectionUnavailable: Boolean,
)
