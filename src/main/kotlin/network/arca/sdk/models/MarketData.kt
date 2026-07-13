package network.arca.sdk.models

import kotlinx.serialization.Serializable

// MARK: - Margin tiers (also used by ActiveAssetData / order breakdown)

@Serializable
public data class MarginTier(
    public val lowerBound: String,
    public val maxLeverage: Int,
)

@Serializable
public data class MarginTable(
    public val description: String,
    public val marginTiers: List<MarginTier>,
)

// MARK: - Market metadata

/** Earliest candle timestamps for an asset. */
@Serializable
public data class CandleHistoryBounds(
    public val earliestMs: Long,
    public val hlEarliestMs: Long,
)

@Serializable
public data class LogoSource(
    public val url: String,
    public val format: String,
    public val width: Int,
)

@Serializable
public data class Market(
    /** Case-sensitive canonical market ID (e.g. "hl:0:BTC", "hl:1:TSLA"). */
    public val name: String,
    public val dex: String? = null,
    /** Display symbol only. Do not reconstruct API coin IDs from this field. */
    public val symbol: String,
    public val venueSymbol: String? = null,
    public val displayName: String? = null,
    public val logoUrl: String? = null,
    public val logoSources: List<LogoSource>? = null,
    public val exchange: String,
    public val assetType: String? = null,
    public val categoryLabel: String? = null,
    public val mapped: Boolean? = null,
    public val hasDisplayName: Boolean? = null,
    public val hasLogo: Boolean? = null,
    public val descriptionStatus: String? = null,
    public val isHip3: Boolean? = null,
    public val deployerDisplayName: String? = null,
    public val index: Int,
    public val szDecimals: Int,
    public val maxLeverage: Int,
    /**
     * Minimum order notional in USD (`size * price`) for this market. Use
     * [Arca.getMinOrderSize] to convert it into a minimum order size in
     * base-asset units, or [Arca.validateOrderSize] to check a size before
     * placing an order. Reduce-only orders and unsized (`sizeToMax`) triggers
     * are exempt. Null when served by an older backend; clients fall back to
     * the venue-wide [Arca.getOrderLimits] default.
     */
    public val minOrderNotionalUsd: Double? = null,
    public val onlyIsolated: Boolean,
    /** Supported margin modes: `["isolated"]` or `["cross", "isolated"]`. */
    public val marginModes: List<String>? = null,
    /** HIP-3 fee multiplier. Null/absent for standard perps (defaults to 1.0). */
    public val feeScale: Double? = null,
    public val marginTableId: Int? = null,
    public val candleHistory: CandleHistoryBounds? = null,
)

@Serializable
public data class SimMetaResponse(
    public val universe: List<Market>,
    public val marginTables: Map<String, MarginTable>? = null,
)

@Serializable
public data class SimMidsResponse(
    public val mids: Map<String, String>,
)

@Serializable
public data class MarketTicker(
    public val market: String,
    public val dex: String? = null,
    public val symbol: String,
    public val exchange: String,
    public val markPx: String,
    public val midPx: String,
    public val prevDayPx: String,
    public val dayNtlVlm: String,
    public val priceChange24hPct: String,
    public val openInterest: String,
    public val funding: String,
    /** Unix timestamp in milliseconds of the next funding event. */
    public val nextFundingTime: Long? = null,
    /** HIP-3 fee multiplier. 1.0 for standard perps. */
    public val feeScale: Double,
    public val isDelisted: Boolean,
)

@Serializable
public data class MarketTickersResponse(
    public val tickers: List<MarketTicker>,
)

@Serializable
public data class SimBookLevel(
    public val price: String,
    public val size: String,
    public val orderCount: Int,
)

@Serializable
public data class SimBookResponse(
    public val market: String,
    public val bids: List<SimBookLevel>,
    public val asks: List<SimBookLevel>,
    public val time: Long,
)

// MARK: - Candles

@Serializable
public data class Candle(
    public val t: Long,
    public val o: String,
    public val h: String,
    public val l: String,
    public val c: String,
    public val v: String,
    public val n: Int,
    /** Data source. Null = venue-native; "ext" = external historical data. */
    public val s: String? = null,
)

@Serializable
public data class CandlesResponse(
    public val market: String,
    public val interval: String,
    public val candles: List<Candle>,
)

/** Emitted by candle streams on each candle change. */
public data class CandleEvent(
    public val market: String,
    public val interval: CandleInterval,
    public val candle: Candle,
)

// MARK: - Open Interest

/**
 * A single open-interest / 24h-notional bar. The OHLC values track open
 * interest (base-asset units) over the bucket; [ntlVlm] is the rolling 24h
 * notional volume (USD) at bucket close; [mark] is the last mark price in the
 * bucket (USD OI ≈ `oiClose * mark`). [s] is the data source (null/"" =
 * self-recorded, "0xa" = 0xArchive backfill).
 */
@Serializable
public data class OIBar(
    public val t: Long,
    public val oiOpen: String,
    public val oiHigh: String,
    public val oiLow: String,
    public val oiClose: String,
    public val ntlVlm: String,
    public val mark: String? = null,
    public val s: String? = null,
)

@Serializable
public data class OIHistoryResponse(
    public val market: String,
    public val interval: String,
    public val bars: List<OIBar>,
)

/** Emitted by open-interest streams on each bar change. */
public data class OIEvent(
    public val market: String,
    public val interval: CandleInterval,
    public val bar: OIBar,
    public val isClosed: Boolean,
)

/** A single trade from the market-wide trade tape. */
@Serializable
public data class MarketTrade(
    public val market: String,
    public val px: String,
    public val sz: String,
    public val side: String,
    public val time: String,
    public val hash: String? = null,
)

/** Callback-friendly trade event. */
public data class TradeEvent(
    public val market: String,
    public val trade: MarketTrade,
)

/** Emitted by a candle-chart stream on every chart change. */
public data class CandleChartUpdate(
    /** Full candle array (historical + live), sorted by `t`, deduped. */
    public val candles: List<Candle>,
    /** The candle that triggered this update. */
    public val latestCandle: Candle,
)

/** Result of a candle-chart range load. */
public data class LoadRangeResult(
    public val loadedCount: Int,
    public val totalCount: Int,
    public val rangeStart: Long,
    public val rangeEnd: Long,
    public val reachedStart: Boolean,
)

// MARK: - Sparklines

@Serializable
public data class SparklinesResponse(
    public val sparklines: Map<String, List<Double>>,
)

// MARK: - Fill / trade history (platform-side)

@Serializable
public data class FillResultingPosition(
    public val side: PositionSide,
    public val size: String,
    public val entryPx: String? = null,
    public val leverage: Int,
)

@Serializable
public data class Fill(
    public val id: String,
    /** Platform operation ID. Absent on preview fills from `fill.previewed`. */
    public val operationId: String? = null,
    public val fillId: String? = null,
    public val orderOperationId: String? = null,
    public val orderId: String? = null,
    public val market: String,
    public val side: OrderSide? = null,
    public val size: String? = null,
    public val price: String? = null,
    public val direction: String? = null,
    public val startPosition: String? = null,
    public val fee: String? = null,
    public val exchangeFee: String? = null,
    public val platformFee: String? = null,
    public val builderFee: String? = null,
    public val realizedPnl: String? = null,
    /** Absent on preview fills; populated by `fill.recorded`. */
    public val resultingPosition: FillResultingPosition? = null,
    public val isLiquidation: Boolean? = null,
    public val createdAt: String? = null,
)

@Serializable
public data class FillListResponse(
    public val fills: List<Fill>,
    public val total: Int,
    public val cursor: String? = null,
)

@Serializable
public data class MarketTradeSummaryItem(
    public val market: String,
    public val totalRealizedPnl: String,
    public val totalFees: String,
    public val tradeCount: Int,
    public val totalVolume: String,
)

@Serializable
public data class TradeSummaryTotals(
    public val totalRealizedPnl: String,
    public val totalFees: String,
    public val tradeCount: Int,
    public val totalVolume: String,
)

@Serializable
public data class TradeSummaryResponse(
    public val markets: List<MarketTradeSummaryItem>,
    public val totals: TradeSummaryTotals,
)
