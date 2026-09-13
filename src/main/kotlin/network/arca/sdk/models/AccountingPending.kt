package network.arca.sdk.models

import java.math.BigDecimal
import kotlinx.serialization.Serializable

/**
 * One venue-confirmed execution that the ledger has not fully recorded into the
 * [ExchangeState] that carries it.
 *
 * The platform confirms an order's execution (the receipt) before its ledger
 * folds the fills into the account, and it folds them one fill at a time.
 * Every observation in between is a real but intermediate account: the old
 * position, a partly projected one, cash debited before margin is released.
 * Rather than make each reader guess whether an observation is "after" an
 * execution it knows about, the observation says so itself: this entry names
 * the execution and exactly how much of it — [unaccountedSize], executed minus
 * accounted — is still missing from `positions`. Adding that signed quantity
 * to the market's position yields the same book on the frame emitted at
 * execution time, on every per-fill frame, and on the settled frame.
 *
 * Positions, side and quantity are exact from the venue and may be composed
 * immediately. Realized P&L, fees and therefore equity are ledger facts; keep
 * the previous money until [ExchangeState.isAccountingSettled] is true.
 */
@Serializable
public data class AccountingPendingExecution(
    public val operationId: String,
    /** The venue order id, null before acknowledgement is recorded. */
    public val orderId: String? = null,
    public val market: String,
    /** `"buy"` or `"sell"`: a buy adds [unaccountedSize] to the market's signed position, a sell subtracts it. */
    public val side: String,
    public val executedSize: String = "0",
    public val accountedSize: String = "0",
    public val unaccountedSize: String,
    /** True once the venue has finished with the order; false for a working order whose fills so far are reported here. */
    public val executionFinal: Boolean = false,
    /** The venue's provisional aggregate price when known, so an opened position can show the price it executed at. */
    public val averagePrice: String? = null,
) {
    /**
     * The quantity this entry adds to its market's signed position (long
     * positive, short negative), or null for an entry that cannot be composed —
     * which a reader treats as "leave this market's ledger row".
     */
    public val signedUnaccounted: BigDecimal?
        get() {
            val size = decimalOrNull(unaccountedSize)?.takeIf { it.signum() >= 0 } ?: return null
            return when (side.lowercase()) {
                "buy", "long" -> size
                "sell", "short" -> size.negate()
                else -> null
            }
        }

    internal companion object {
        private val syntax = Regex("^-?[0-9]+(?:\\.[0-9]+)?$")
        internal fun decimalOrNull(raw: String?): BigDecimal? {
            val text = raw?.trim().orEmpty()
            if (!syntax.matches(text)) return null
            return text.toBigDecimalOrNull()
        }
    }
}

/** Where a [ProjectedPosition]'s quantity came from. */
public enum class ProjectedPositionSource {
    /** No pending execution touches this market; the row is the ledger position verbatim. */
    LEDGER,
    /**
     * At least one pending execution was composed onto the ledger position (or
     * onto a flat market). Quantity and side are exact; money on
     * [ProjectedPosition.ledger], if any, describes the pre-execution row.
     */
    EXECUTION,
}

/** One market of the composed book: the ledger position with every pending execution for that market applied. */
public data class ProjectedPosition(
    public val market: String,
    public val side: PositionSide,
    /** The unsigned composed quantity. */
    public val size: BigDecimal,
    /**
     * The ledger entry for an unchanged or reduced position, the quantity-weighted
     * blend for an increase whose executions carried an average price, the
     * execution's average price for a position the pending executions opened
     * or reversed, and null when it cannot be known.
     */
    public val entryPrice: BigDecimal?,
    public val source: ProjectedPositionSource,
    /** The observation's own row for this market, null when the market was flat on the ledger. */
    public val ledger: SimPosition?,
    /** The executions composed onto this market. */
    public val pending: List<AccountingPendingExecution>,
)

/**
 * Whether this observation's positions and money include every execution the
 * platform knows about. While false, show [projectedPositions] and keep the
 * previously settled balances.
 */
public val ExchangeState.isAccountingSettled: Boolean
    get() = accountingPending.isNullOrEmpty()

/**
 * [ExchangeState.positions] composed with [ExchangeState.accountingPending]:
 * the book as it will read once the ledger has recorded everything the venue
 * has already executed. Markets that compose to zero are omitted. Ledger
 * positions keep their order, followed by positions the pending executions
 * opened, in market order. A market whose pending entries cannot be parsed is
 * returned as its ledger row with [ProjectedPositionSource.LEDGER], so a
 * malformed entry can never invent or erase a position.
 */
public fun ExchangeState.projectedPositions(): List<ProjectedPosition> {
    val pendingByMarket = accountingPending.orEmpty().groupBy { it.market }
    val out = mutableListOf<ProjectedPosition>()
    val seen = mutableSetOf<String>()
    for (position in positions) {
        seen += position.market
        val ledgerRow = ProjectedPosition(position.market, position.side,
            AccountingPendingExecution.decimalOrNull(position.size)?.abs() ?: BigDecimal.ZERO,
            AccountingPendingExecution.decimalOrNull(position.entryPrice), ProjectedPositionSource.LEDGER, position, emptyList())
        val pending = pendingByMarket[position.market]
        if (pending == null) { out += ledgerRow; continue }
        when (val composed = composeProjectedPosition(position, pending)) {
            Composition.Failure -> out += ledgerRow
            Composition.Flat -> Unit
            is Composition.Position -> out += composed.row
        }
    }
    for (market in pendingByMarket.keys.sorted()) {
        if (market in seen) continue
        val composed = composeProjectedPosition(null, pendingByMarket.getValue(market))
        if (composed is Composition.Position) out += composed.row
    }
    return out
}

private sealed interface Composition {
    data object Failure : Composition
    data object Flat : Composition
    data class Position(val row: ProjectedPosition) : Composition
}

private fun composeProjectedPosition(ledger: SimPosition?, pending: List<AccountingPendingExecution>): Composition {
    var signed = BigDecimal.ZERO
    var entry: BigDecimal? = null
    var market = ledger?.market.orEmpty()
    if (ledger != null) {
        val size = AccountingPendingExecution.decimalOrNull(ledger.size) ?: return Composition.Failure
        signed = if (ledger.side == PositionSide.SHORT) size.negate() else size
        entry = AccountingPendingExecution.decimalOrNull(ledger.entryPrice)?.takeIf { it.signum() > 0 }
    }
    for (execution in pending) {
        val delta = execution.signedUnaccounted ?: return Composition.Failure
        if (market.isEmpty()) market = execution.market
        entry = blendedEntry(signed, entry, delta, execution.averagePrice)
        signed += delta
    }
    if (signed.signum() == 0) return Composition.Flat
    return Composition.Position(ProjectedPosition(market, if (signed.signum() > 0) PositionSide.LONG else PositionSide.SHORT,
        signed.abs(), entry, ProjectedPositionSource.EXECUTION, ledger, pending))
}

/**
 * The entry price after applying [delta] to [held] at [entry]: unchanged for a
 * same-side reduction, quantity-weighted for an increase with a known execution
 * price, the execution price for an open or the far side of a reversal, and
 * null when it cannot be derived.
 */
private fun blendedEntry(held: BigDecimal, entry: BigDecimal?, delta: BigDecimal, averagePrice: String?): BigDecimal? {
    val price = AccountingPendingExecution.decimalOrNull(averagePrice)?.takeIf { it.signum() > 0 }
    val next = held + delta
    if (held.signum() == 0 || next.signum() == 0 || held.signum() != next.signum()) return price
    if (delta.signum() != held.signum()) return entry
    if (entry == null || price == null) return null
    val heldAbs = held.abs()
    val deltaAbs = delta.abs()
    return (heldAbs * entry + deltaAbs * price).divide(heldAbs + deltaAbs, java.math.MathContext.DECIMAL128)
}
