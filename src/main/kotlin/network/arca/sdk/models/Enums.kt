package network.arca.sdk.models

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/*
 * Wire-mapped enums.
 *
 * Closed enums (a fixed server vocabulary) are real Kotlin `enum class`es with
 * `@SerialName` driving (de)serialization and a `wire` property for code that
 * needs the raw string. "Open" enums whose server vocabulary may grow are
 * inline value classes with companion constants, so an unrecognized value round
 * trips instead of throwing — the Kotlin analogue of Swift's
 * `case unknown(String)`.
 */

// MARK: - Operation

/** An "open" operation type: unknown server values are preserved verbatim. */
@Serializable
@JvmInline
public value class OperationType(public val value: String) {
    override fun toString(): String = value

    public companion object {
        public val TRANSFER: OperationType = OperationType("transfer")
        public val CREATE: OperationType = OperationType("create")
        public val DELETE: OperationType = OperationType("delete")
        public val DEPOSIT: OperationType = OperationType("deposit")
        public val WITHDRAWAL: OperationType = OperationType("withdrawal")
        public val SWAP: OperationType = OperationType("swap")
        public val ORDER: OperationType = OperationType("order")
        public val FILL: OperationType = OperationType("fill")
        public val CANCEL: OperationType = OperationType("cancel")
        public val MODIFY: OperationType = OperationType("modify")
        public val FEE_DISTRIBUTION: OperationType = OperationType("fee_distribution")
        public val ADJUSTMENT: OperationType = OperationType("adjustment")
        public val FUNDING: OperationType = OperationType("funding")
        public val VENUE_CLOSE: OperationType = OperationType("venue_close")
        public val TWAP: OperationType = OperationType("twap")
    }
}

@Serializable
public enum class OperationState(public val wire: String) {
    @SerialName("pending") PENDING("pending"),
    @SerialName("completed") COMPLETED("completed"),
    @SerialName("failed") FAILED("failed"),
    @SerialName("expired") EXPIRED("expired"),
    ;

    /** True for `completed`, `failed`, and `expired`. */
    public val isTerminal: Boolean
        get() = this != PENDING
}

// MARK: - State delta

@Serializable
@JvmInline
public value class DeltaType(public val value: String) {
    override fun toString(): String = value

    public companion object {
        public val BALANCE_CHANGE: DeltaType = DeltaType("balance_change")
        public val BALANCE_ADJUSTMENT: DeltaType = DeltaType("balance_adjustment")
        public val SETTLEMENT_CHANGE: DeltaType = DeltaType("settlement_change")
        public val POSITION_CHANGE: DeltaType = DeltaType("position_change")
        public val STATUS_CHANGE: DeltaType = DeltaType("status_change")
        public val HOLD_CHANGE: DeltaType = DeltaType("hold_change")
        public val LABELS_CHANGE: DeltaType = DeltaType("labels_change")
        public val CREATION: DeltaType = DeltaType("creation")
        public val DELETION: DeltaType = DeltaType("deletion")
    }
}

// MARK: - Arca object

@Serializable
@JvmInline
public value class ArcaObjectType(public val value: String) {
    override fun toString(): String = value

    public companion object {
        public val DENOMINATED: ArcaObjectType = ArcaObjectType("denominated")
        public val EXCHANGE: ArcaObjectType = ArcaObjectType("exchange")
        public val DEPOSIT: ArcaObjectType = ArcaObjectType("deposit")
        public val WITHDRAWAL: ArcaObjectType = ArcaObjectType("withdrawal")
        public val ESCROW: ArcaObjectType = ArcaObjectType("escrow")
        public val INFO: ArcaObjectType = ArcaObjectType("info")
    }
}

@Serializable
public enum class ArcaObjectStatus(public val wire: String) {
    @SerialName("active") ACTIVE("active"),
    @SerialName("deleting") DELETING("deleting"),
    @SerialName("deleted") DELETED("deleted"),
}

@Serializable
public enum class BoundaryStatus(public val wire: String) {
    @SerialName("active") ACTIVE("active"),
    @SerialName("soft_frozen") SOFT_FROZEN("soft_frozen"),
    @SerialName("hard_frozen") HARD_FROZEN("hard_frozen"),
}

// MARK: - Reserved balances

@Serializable
public enum class ReservedBalanceStatus(public val wire: String) {
    @SerialName("held") HELD("held"),
    @SerialName("released") RELEASED("released"),
    @SerialName("cancelled") CANCELLED("cancelled"),
}

@Serializable
public enum class ReservedBalanceDirection(public val wire: String) {
    @SerialName("inbound") INBOUND("inbound"),
    @SerialName("outbound") OUTBOUND("outbound"),
}

// MARK: - Realm

@Serializable
public enum class RealmType(public val wire: String) {
    @SerialName("development") DEVELOPMENT("development"),
    @SerialName("production") PRODUCTION("production"),
}

@Serializable
public enum class RealmAsset(public val wire: String) {
    @SerialName("paper") PAPER("paper"),
    @SerialName("live") LIVE("live"),
}

@Serializable
public enum class RealmLifecycle(public val wire: String) {
    @SerialName("permanent") PERMANENT("permanent"),
    @SerialName("temporary") TEMPORARY("temporary"),
}

@Serializable
public enum class RealmBacking(public val wire: String) {
    @SerialName("chain") CHAIN("chain"),
    @SerialName("ledger-only") LEDGER_ONLY("ledger-only"),
}

// MARK: - Exchange / trading

@Serializable
public enum class OrderSide(public val wire: String) {
    @SerialName("buy") BUY("buy"),
    @SerialName("sell") SELL("sell"),
    ;

    public companion object {
        public fun fromWire(value: String): OrderSide? = entries.firstOrNull { it.wire == value }
    }
}

@Serializable
public enum class PositionSide(public val wire: String) {
    @SerialName("long") LONG("long"),
    @SerialName("short") SHORT("short"),
    ;

    public companion object {
        public fun fromWire(value: String): PositionSide? = entries.firstOrNull { it.wire == value }
    }
}

@Serializable
public enum class OrderType(public val wire: String) {
    @SerialName("MARKET") MARKET("MARKET"),
    @SerialName("LIMIT") LIMIT("LIMIT"),
}

@Serializable
public enum class OrderStatus(public val wire: String) {
    @SerialName("PENDING") PENDING("PENDING"),
    @SerialName("OPEN") OPEN("OPEN"),
    @SerialName("PARTIALLY_FILLED") PARTIALLY_FILLED("PARTIALLY_FILLED"),
    @SerialName("FILLED") FILLED("FILLED"),
    @SerialName("CANCELLED") CANCELLED("CANCELLED"),
    @SerialName("FAILED") FAILED("FAILED"),
    @SerialName("WAITING_FOR_TRIGGER") WAITING_FOR_TRIGGER("WAITING_FOR_TRIGGER"),
    @SerialName("TRIGGERED") TRIGGERED("TRIGGERED"),
}

@Serializable
public enum class TpslType(public val wire: String) {
    @SerialName("tp") TAKE_PROFIT("tp"),
    @SerialName("sl") STOP_LOSS("sl"),
}

@Serializable
public enum class LeverageType(public val wire: String) {
    @SerialName("cross") CROSS("cross"),
    @SerialName("isolated") ISOLATED("isolated"),
}

@Serializable
public enum class MarginMode(public val wire: String) {
    @SerialName("cross") CROSS("cross"),
    @SerialName("isolated") ISOLATED("isolated"),
}

@Serializable
public enum class TimeInForce(public val wire: String) {
    @SerialName("GTC") GTC("GTC"),
    @SerialName("IOC") IOC("IOC"),
    @SerialName("ALO") ALO("ALO"),
}

// MARK: - TWAP

@Serializable
public enum class TwapStatus(public val wire: String) {
    @SerialName("active") ACTIVE("active"),
    @SerialName("completed") COMPLETED("completed"),
    @SerialName("cancelled") CANCELLED("cancelled"),
    @SerialName("failed") FAILED("failed"),
}

@Serializable
public enum class TwapType(public val wire: String) {
    @SerialName("twap") TWAP("twap"),
    @SerialName("dca") DCA("dca"),
}

// MARK: - Payment links

@Serializable
public enum class PaymentLinkType(public val wire: String) {
    @SerialName("deposit") DEPOSIT("deposit"),
    @SerialName("withdrawal") WITHDRAWAL("withdrawal"),
}

// MARK: - Aggregation

@Serializable
public enum class AssetCategory(public val wire: String) {
    @SerialName("spot") SPOT("spot"),
    @SerialName("perp") PERP("perp"),
    @SerialName("exchange") EXCHANGE("exchange"),
}

/**
 * Indicates which side owns an object's price-derived valuation. `CLIENT`
 * (the default, and the value when absent on the wire) means the SDK recomputes
 * price-derived fields locally from raw mids; `SERVER` means those fields are
 * authoritative as delivered and must not be recomputed.
 */
@Serializable
public enum class PricingMode(public val wire: String) {
    @SerialName("client") CLIENT("client"),
    @SerialName("server") SERVER("server"),
}

@Serializable
public enum class AggregationSourceType(public val wire: String) {
    @SerialName("prefix") PREFIX("prefix"),
    @SerialName("pattern") PATTERN("pattern"),
    @SerialName("paths") PATHS("paths"),
    @SerialName("watch") WATCH("watch"),
}

@Serializable
public enum class ChartPointStatus(public val wire: String) {
    @SerialName("open") OPEN("open"),
    @SerialName("sealed") SEALED("sealed"),
    @SerialName("carried") CARRIED("carried"),
    @SerialName("incomplete") INCOMPLETE("incomplete"),
}

// MARK: - Candles

@Serializable
public enum class CandleInterval(public val wire: String, public val milliseconds: Long) {
    @SerialName("15s") FIFTEEN_SECONDS("15s", 15_000),
    @SerialName("1m") ONE_MINUTE("1m", 60_000),
    @SerialName("5m") FIVE_MINUTES("5m", 300_000),
    @SerialName("15m") FIFTEEN_MINUTES("15m", 900_000),
    @SerialName("1h") ONE_HOUR("1h", 3_600_000),
    @SerialName("4h") FOUR_HOURS("4h", 14_400_000),
    @SerialName("1d") ONE_DAY("1d", 86_400_000),
    ;

    public companion object {
        public fun fromWire(value: String): CandleInterval? = entries.firstOrNull { it.wire == value }
    }
}
