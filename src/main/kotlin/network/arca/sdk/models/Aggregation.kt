package network.arca.sdk.models

import kotlinx.serialization.Serializable
import network.arca.sdk.ObjectId
import network.arca.sdk.OperationId
import network.arca.sdk.WatchId

@Serializable
public data class AssetBreakdown(
    public val asset: String,
    public val category: AssetCategory,
    public val amount: String,
    public val price: String? = null,
    public val valueUsd: String,
    public val weightedAvgLeverage: String? = null,
    public val avgEntryPrice: String? = null,
)

@Serializable
public data class BalanceValue(
    public val denomination: String,
    public val amount: String,
    public val price: String? = null,
    public val valueUsd: String,
)

@Serializable
public data class PositionValue(
    public val market: String,
    public val side: String,
    public val size: String,
    public val entryPrice: String,
    public val markPrice: String? = null,
    public val unrealizedPnl: String? = null,
    public val valueUsd: String? = null,
)

@Serializable
public data class ReservedValue(
    public val denomination: String,
    public val amount: String,
    public val price: String? = null,
    public val valueUsd: String,
    public val operationId: OperationId,
    public val sourceArcaPath: String? = null,
    public val destinationArcaPath: String? = null,
    public val startedAt: String? = null,
    public val inTransit: Boolean? = null,
)

@Serializable
public data class ObjectValuation(
    public val objectId: ObjectId,
    public val path: String,
    public val type: String,
    public val denomination: String? = null,
    public val valueUsd: String,
    public val balances: List<BalanceValue> = emptyList(),
    public val reservedBalances: List<ReservedValue>? = null,
    public val pendingInbound: List<ReservedValue>? = null,
    public val positions: List<PositionValue>? = null,
    /** When [PricingMode.SERVER], price-derived fields are server-authoritative. */
    public val pricingMode: PricingMode? = null,
)

@Serializable
public data class PathAggregation(
    public val prefix: String,
    public val totalEquityUsd: String,
    public val departingUsd: String,
    public val arrivingUsd: String? = null,
    public val breakdown: List<AssetBreakdown> = emptyList(),
    public val asOf: String? = null,
    public val cumInflowsUsd: String? = null,
    public val cumOutflowsUsd: String? = null,
    /** When [PricingMode.SERVER], totals are server-authoritative. */
    public val pricingMode: PricingMode? = null,
)

// MARK: - Aggregation source

@Serializable
public data class AggregationSource(
    public val type: AggregationSourceType,
    public val value: String,
)

@Serializable
public data class CreateWatchResponse(
    public val watchId: WatchId,
    public val aggregation: PathAggregation,
)

// MARK: - P&L

@Serializable
public data class ExternalFlowEntry(
    public val operationId: OperationId,
    public val type: String,
    public val direction: String,
    public val amount: String,
    public val denomination: String,
    public val valueUsd: String,
    public val sourceArcaPath: String? = null,
    public val targetArcaPath: String? = null,
    public val timestamp: String,
)

@Serializable
public data class PnlResponse(
    public val prefix: String,
    public val from: String,
    public val to: String,
    public val startingEquityUsd: String,
    public val endingEquityUsd: String,
    public val netInflowsUsd: String,
    public val netOutflowsUsd: String,
    public val pnlUsd: String,
    public val externalFlows: List<ExternalFlowEntry>? = null,
)

// MARK: - P&L history

@Serializable
public data class PnlPoint(
    public val timestamp: String,
    public val pnlUsd: String,
    public val equityUsd: String,
    public val status: ChartPointStatus? = null,
    public val cumInflowsUsd: String? = null,
    public val cumOutflowsUsd: String? = null,
    public val lastEventOpId: String? = null,
    public val midSetId: String? = null,
    /** Present when the chart is created with [PnlAnchor.EQUITY]. */
    public var valueUsd: String? = null,
)

/** Controls the y-axis baseline for P&L charts. */
public enum class PnlAnchor {
    /** Standard P&L chart starting at 0. */
    ZERO,

    /** P&L shifted so the live value equals current account equity. */
    EQUITY,
}

@Serializable
public data class PnlHistoryResponse(
    public val prefix: String,
    public val from: String,
    public val to: String,
    public val points: Int,
    public val resolution: String? = null,
    public val resolutionRequested: String? = null,
    public val serverNow: String? = null,
    public val startingEquityUsd: String,
    /** Timestamp of the first non-zero equity point (after leading-zero trimming). */
    public val effectiveFrom: String? = null,
    public val pnlPoints: List<PnlPoint> = emptyList(),
    public val externalFlows: List<ExternalFlowEntry>? = null,
    public val midPrices: Map<String, String>? = null,
)

// MARK: - Equity history

@Serializable
public data class EquityPoint(
    public val timestamp: String,
    public val equityUsd: String,
    public val status: ChartPointStatus? = null,
    public val cumInflowsUsd: String? = null,
    public val cumOutflowsUsd: String? = null,
    public val lastEventOpId: String? = null,
    public val midSetId: String? = null,
)

@Serializable
public data class EquityHistoryResponse(
    public val prefix: String,
    public val from: String,
    public val to: String,
    public val points: Int,
    public val resolution: String? = null,
    public val resolutionRequested: String? = null,
    public val serverNow: String? = null,
    public val equityPoints: List<EquityPoint> = emptyList(),
)

/** Emitted by an equity-chart stream on each update. */
public data class EquityChartUpdate(
    public val points: List<EquityPoint>,
)

/** Emitted by a P&L-chart stream on each update. */
public data class PnlChartUpdate(
    public val points: List<PnlPoint>,
    public val externalFlows: List<ExternalFlowEntry>,
)
