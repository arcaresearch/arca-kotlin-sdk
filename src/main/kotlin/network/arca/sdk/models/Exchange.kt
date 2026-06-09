package network.arca.sdk.models

import kotlinx.serialization.Serializable
import network.arca.sdk.OperationResponse
import network.arca.sdk.RealmId
import network.arca.sdk.SimAccountId
import network.arca.sdk.SimFillId
import network.arca.sdk.SimOrderId
import network.arca.sdk.SimPositionId

// MARK: - Exchange state

@Serializable
public data class SimAccount(
    public val id: SimAccountId,
    public val realmId: RealmId,
    public val name: String,
    public val createdAt: String,
    public val updatedAt: String,
)

@Serializable
public data class SimMarginSummary(
    public val equity: String,
    public val initialMarginUsed: String,
    public val maintenanceMarginRequired: String,
    public val availableToWithdraw: String,
    public val totalNtlPos: String,
    public val totalUnrealizedPnl: String,
    public val totalRawUsd: String? = null,
)

@Serializable
public data class SimPosition(
    public val id: SimPositionId,
    public val accountId: SimAccountId? = null,
    public val realmId: RealmId? = null,
    public val market: String,
    public val side: PositionSide,
    public val size: String,
    public val entryPrice: String,
    public val leverage: Int,
    public val marginUsed: String,
    /** Margin mode. Isolated positions carry their own collateral. */
    public val marginMode: MarginMode = MarginMode.CROSS,
    /** Locked collateral for an isolated position; null for cross positions. */
    public val isolatedMargin: String? = null,
    public val liquidationPrice: String? = null,
    public val unrealizedPnl: String? = null,
    public val returnOnEquity: String? = null,
    public val positionValue: String? = null,
    public val error: String? = null,
    public val cumulativeFunding: String? = null,
    public val cumulativeFee: String? = null,
    public val cumulativeExchangeFee: String? = null,
    public val cumulativePlatformFee: String? = null,
    public val cumulativeBuilderFee: String? = null,
    public val createdAt: String? = null,
    public val updatedAt: String? = null,
)

@Serializable
internal data class PositionListResponse(
    val positions: List<SimPosition>,
    val total: Int,
)

@Serializable
internal data class OrderListResponse(
    val orders: List<SimOrder>,
    val total: Int,
)

@Serializable
public data class SimOrder(
    public val id: SimOrderId,
    public val accountId: SimAccountId? = null,
    public val realmId: RealmId? = null,
    public val market: String,
    public val side: OrderSide,
    public val orderType: OrderType,
    public val price: String? = null,
    public val size: String,
    public val filledSize: String,
    public val avgFillPrice: String? = null,
    public val status: OrderStatus,
    public val reduceOnly: Boolean,
    public val timeInForce: TimeInForce,
    public val leverage: Int,
    public val builderFeeBps: Int? = null,
    public val isTrigger: Boolean? = null,
    public val triggerPx: String? = null,
    public val isMarket: Boolean? = null,
    public val tpsl: String? = null,
    /** True for an unsized ("size to max") TP/SL that closes the whole position. */
    public val sizeToMax: Boolean? = null,
    /** Links the legs of a TP/SL bracket for one-cancels-the-other behavior. */
    public val ocoGroupId: String? = null,
    /** Why a cancelled order was cancelled. Null unless `status == CANCELLED`. */
    public val cancelReason: String? = null,
    public val createdAt: String? = null,
    public val updatedAt: String? = null,
) {
    /** True when the order reached a terminal status and has at least one fill. */
    public val isTerminalWithFills: Boolean
        get() = when (status) {
            OrderStatus.FILLED -> true
            OrderStatus.CANCELLED -> filledSize != "0" && filledSize.isNotEmpty()
            else -> false
        }

    /** True when partially filled and the remainder cancelled (IOC semantics). */
    public val isPartiallyFilled: Boolean
        get() = status == OrderStatus.CANCELLED && filledSize != "0" && filledSize.isNotEmpty() && filledSize != size

    /** True when this is a trigger (TP/SL) order. */
    public val isTriggerOrder: Boolean
        get() = isTrigger == true
}

@Serializable
public data class SimFill(
    public val id: SimFillId,
    public val orderId: SimOrderId,
    public val accountId: SimAccountId? = null,
    public val realmId: RealmId? = null,
    public val market: String,
    public val side: OrderSide,
    public val price: String,
    public val size: String,
    public val fee: String,
    public val builderFee: String? = null,
    public val platformFee: String? = null,
    public val realizedPnl: String? = null,
    public val isLiquidation: Boolean,
    public val createdAt: String? = null,
)

@Serializable
public data class FundingPayment(
    public val accountId: String,
    public val market: String,
    public val side: String,
    public val size: String,
    public val price: String,
    public val fundingRate: String,
    public val payment: String,
)

@Serializable
public data class SimFeeTierEntry(
    public val tier: Int,
    public val label: String,
    public val minVolume14d: Int,
    public val takerBps: Int,
    public val makerBps: Int,
)

@Serializable
public data class SimFeeRates(
    public val taker: String,
    public val maker: String,
    public val platformFee: String? = null,
    public val tier: Int? = null,
    public val tierLabel: String? = null,
    public val volume14d: String? = null,
    public val schedule: List<SimFeeTierEntry>? = null,
)

/** A pending order operation projected as a structured intent. */
@Serializable
public data class ExchangeIntent(
    public val operationId: String,
    public val operationPath: String,
    public val market: String,
    public val side: String,
    public val size: String,
    public val orderType: String,
    public val reduceOnly: Boolean,
    public val createdAt: String,
)

@Serializable
public data class ExchangeState(
    public val account: SimAccount,
    public val marginSummary: SimMarginSummary,
    public val crossMarginSummary: SimMarginSummary? = null,
    public val crossMaintenanceMarginUsed: String? = null,
    public val positions: List<SimPosition> = emptyList(),
    public val openOrders: List<SimOrder> = emptyList(),
    public val feeRates: SimFeeRates? = null,
    /** Pending order operations that haven't settled yet. */
    public val pendingIntents: List<ExchangeIntent>? = null,
    /**
     * When [PricingMode.SERVER], price-derived fields are server-authoritative
     * and the SDK does not recompute them from mids. Absent ⇒ [PricingMode.CLIENT].
     */
    public val pricingMode: PricingMode? = null,
)

@Serializable
public data class SimOrderWithFills(
    public val order: SimOrder,
    public val fills: List<SimFill>,
)

// MARK: - Active asset data

@Serializable
public data class LeverageInfo(
    public val type: LeverageType,
    public val value: Int,
)

@Serializable
public data class ActiveAssetData(
    public val market: String,
    public val leverage: LeverageInfo,
    public val maxBuySize: String,
    public val maxSellSize: String,
    public val maxBuyUsd: String,
    public val maxSellUsd: String,
    public val availableToTrade: String,
    public val markPx: String,
    public val feeRate: String,
    public val maintenanceMarginRate: String,
    public val marginTiers: List<MarginTier>? = null,
    public val bidPx: String? = null,
    public val askPx: String? = null,
)

/** Per-asset fee rate entry returned by `getAssetFees`. */
@Serializable
public data class AssetFeeEntry(
    public val market: String,
    public val takerFeeRate: String,
    public val makerFeeRate: String,
)

// MARK: - Order breakdown (pure-client calculator types)

/** How `amount` should be interpreted by `orderBreakdown`. */
public enum class OrderBreakdownAmountType(public val wire: String) {
    SPEND("spend"),
    NOTIONAL("notional"),
    TOKENS("tokens"),
}

/** Existing same-coin position passed via [OrderBreakdownAccountContext]. */
public data class OrderBreakdownExistingPosition(
    public val side: PositionSide,
    public val size: String,
    public val entryPrice: String,
)

/** Account-wide context required to produce a cross-margin liquidation estimate. */
public data class OrderBreakdownAccountContext(
    public val equity: String,
    public val otherMaintenanceMargin: String,
    public val existingPosition: OrderBreakdownExistingPosition? = null,
)

/**
 * Input options for `orderBreakdown`. When [maintenanceMarginRate] is provided,
 * [accountContext] must also be provided so the returned
 * `estimatedLiquidationPrice` reflects cross-margin reality.
 */
public data class OrderBreakdownOptions(
    public val amount: String,
    public val amountType: OrderBreakdownAmountType,
    public val leverage: Int,
    public val feeRate: String,
    public val price: String,
    public val side: OrderSide,
    public val szDecimals: Int = 5,
    public val maintenanceMarginRate: String? = null,
    public val accountContext: OrderBreakdownAccountContext? = null,
    public val marginTiers: List<MarginTier>? = null,
)

/** Result of `orderBreakdown`. */
public data class OrderBreakdown(
    public val tokens: String,
    public val notionalUsd: String,
    public val marginRequired: String,
    public val estimatedFee: String,
    public val totalSpend: String,
    public val price: String,
    public val feeRate: String,
    public val effectiveLeverage: String? = null,
    public val effectiveMaintenanceMarginRate: String? = null,
    public val nextTierThreshold: String? = null,
    public val estimatedLiquidationPrice: String? = null,
)

// MARK: - Leverage

@Serializable
public data class UpdateLeverageResponse(
    public val accountId: String,
    public val market: String,
    public val leverage: Int,
    public val previousLeverage: Int,
)

@Serializable
public data class LeverageSetting(
    public val market: String,
    public val leverage: Int,
    public val marginMode: MarginMode,
)

@Serializable
public data class UpdateIsolatedMarginResponse(
    public val accountId: String,
    public val market: String,
    public val isolatedMargin: String,
    public val liquidationPrice: String,
)

@Serializable
public data class SetMarginModeResponse(
    public val accountId: String,
    public val market: String,
    public val marginMode: MarginMode,
)

// MARK: - Order operation

@Serializable
public data class OrderOperationResponse(
    override val operation: Operation,
) : OperationResponse {
    override fun withOperation(operation: Operation): OrderOperationResponse = copy(operation = operation)
}

// MARK: - Fee target

@Serializable
public data class FeeTarget(
    public val arcaPath: String,
    public val percentage: Int,
)
