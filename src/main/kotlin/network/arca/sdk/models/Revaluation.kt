package network.arca.sdk.models

import network.arca.sdk.internal.decToString
import network.arca.sdk.internal.parseDecimalOrNull
import network.arca.sdk.internal.parseDecimalOrZero
import java.math.BigDecimal
import java.math.MathContext

// MARK: - Client-side revaluation
//
// Mirrors the Swift SDK's `revalued(with:)` family. Price-derived fields are
// recomputed locally from raw mid prices (Axiom 10: Observational Consistency)
// so live updates reflect current prices without extra server bandwidth.

/** Returns a copy with `valueUsd` and `price` recomputed from current mid prices. */
public fun BalanceValue.revalued(mids: Map<String, String>): BalanceValue {
    val mid = mids[denomination] ?: "1"
    val amountDec = parseDecimalOrZero(amount)
    val priceDec = parseDecimalOrNull(mid) ?: BigDecimal.ONE
    val value = amountDec * priceDec
    return copy(price = mid, valueUsd = decToString(value))
}

/** Returns a copy with `markPrice`, `unrealizedPnl`, and `valueUsd` recomputed. */
public fun PositionValue.revalued(mids: Map<String, String>): PositionValue {
    val mid = mids[market] ?: return this
    val markDec = parseDecimalOrNull(mid) ?: return this
    val sizeDec = parseDecimalOrZero(size)
    val entryDec = parseDecimalOrZero(entryPrice)
    val signedSize = if (side == "short") sizeDec.negate() else sizeDec
    val pnl = signedSize * (markDec - entryDec)
    return copy(markPrice = mid, unrealizedPnl = decToString(pnl), valueUsd = decToString(pnl))
}

/** Returns a copy with `valueUsd` and `price` recomputed from current mid prices. */
public fun ReservedValue.revalued(mids: Map<String, String>): ReservedValue {
    val mid = mids[denomination] ?: "1"
    val amountDec = parseDecimalOrZero(amount)
    val priceDec = parseDecimalOrNull(mid) ?: BigDecimal.ONE
    val value = amountDec * priceDec
    return copy(price = mid, valueUsd = decToString(value))
}

/**
 * Returns a copy with all price-derived fields recomputed from mid prices.
 * Static data (amounts, sizes, entry prices, paths) is preserved.
 */
public fun ObjectValuation.revalued(mids: Map<String, String>): ObjectValuation {
    // Server-authoritative pricing: trust the server's values verbatim.
    if (pricingMode == PricingMode.SERVER) return this
    if (type == "exchange") {
        val newPositions = positions?.map { it.revalued(mids) }
        val cashStr = balances.firstOrNull()?.amount ?: "0"
        val cashDec = parseDecimalOrZero(cashStr)
        val totalPnl = newPositions?.fold(BigDecimal.ZERO) { sum, pos ->
            sum + parseDecimalOrZero(pos.unrealizedPnl ?: "0")
        } ?: BigDecimal.ZERO
        val equity = cashDec + totalPnl
        return copy(
            valueUsd = decToString(equity),
            reservedBalances = reservedBalances?.map { it.revalued(mids) },
            pendingInbound = pendingInbound?.map { it.revalued(mids) },
            positions = newPositions,
        )
    }

    val newBalances = balances.map { it.revalued(mids) }
    val objValue = newBalances.fold(BigDecimal.ZERO) { sum, b -> sum + parseDecimalOrZero(b.valueUsd) }
    return copy(
        valueUsd = decToString(objValue),
        balances = newBalances,
        reservedBalances = reservedBalances?.map { it.revalued(mids) },
        pendingInbound = pendingInbound?.map { it.revalued(mids) },
    )
}

/**
 * Returns a copy re-marked at the given mids, in exact decimals.
 *
 * One rule covers every price-derived figure: `equity`, `unrealizedValue` and
 * the windowed `pnl` each shift by the change in position P&L from the
 * delivered row to the new mids. Everything else the server put in them
 * (cash, unsettled funding, the P&L baseline) is kept as delivered. `volume`
 * is never re-marked.
 *
 * Rows the server prices ([PricingMode.SERVER]), rows without positions, and
 * positions without a mid pass through unchanged. Always re-mark the row as
 * delivered, never an already re-marked one.
 */
public fun ProjectedValuation.revalued(mids: Map<String, String>): ProjectedValuation {
    if (pricingMode == PricingMode.SERVER) return this
    val positions = positions
    if (positions.isNullOrEmpty()) return this

    var shift = BigDecimal.ZERO
    var moved = false
    var allMarked = true
    val newPositions = positions.map { p ->
        val remarked = p.remarkedWithShift(mids)
        if (remarked == null) {
            if (p.markPrice == null) allMarked = false
            p
        } else {
            moved = true
            shift += remarked.second
            remarked.first
        }
    }
    if (!moved) return this

    fun shifted(raw: String?): String? {
        val value = parseDecimalOrNull(raw) ?: return raw
        return decToString(value + shift)
    }
    val newPnl = pnl?.let { p ->
        if (p.windowedStatus == WindowedStatus.PENDING) p
        else parseDecimalOrNull(p.pnlUsd)?.let { p.copy(pnlUsd = decToString(it + shift)) } ?: p
    }
    return copy(
        midsComplete = if (midsComplete == false && allMarked) null else midsComplete,
        equity = shifted(equity),
        unrealizedValue = shifted(unrealizedValue),
        positions = newPositions,
        pnl = newPnl,
    )
}

/**
 * Re-marks a projected position at a new mid: the position and the change in
 * its P&L from the delivered row, or null when the mid or the position's
 * numbers are unusable. The change is measured from the delivered mark; a
 * position the server could not mark was left out of its row's figures, so
 * its whole P&L is the change.
 */
private fun PositionValue.remarkedWithShift(mids: Map<String, String>): Pair<PositionValue, BigDecimal>? {
    val mid = mids[market] ?: return null
    val midDec = parseDecimalOrNull(mid) ?: return null
    val sizeDec = parseDecimalOrNull(size) ?: return null
    val signedSize = when (side) {
        "short" -> sizeDec.abs().negate()
        "long" -> sizeDec.abs()
        else -> sizeDec
    }
    val delivered = parseDecimalOrNull(unrealizedPnl)
    val markDec = parseDecimalOrNull(markPrice)
    val pnl: BigDecimal
    val shift: BigDecimal
    if (markDec != null && delivered != null) {
        shift = signedSize * (midDec - markDec)
        pnl = delivered + shift
    } else {
        val entryDec = parseDecimalOrNull(entryPrice) ?: return null
        pnl = signedSize * (midDec - entryDec)
        shift = pnl - (delivered ?: BigDecimal.ZERO)
    }
    val pnlStr = decToString(pnl)
    return copy(markPrice = mid, unrealizedPnl = pnlStr, valueUsd = pnlStr) to shift
}

/**
 * Returns a copy with totals recomputed from [PathAggregation.breakdown] using
 * mid prices. Spot rows use `amount × mid`; perp rows recompute mark-to-market
 * P&L. Exchange rows keep server `AssetBreakdown.valueUsd`. `departingUsd` and
 * `arrivingUsd` are USD-denominated and pass through unchanged.
 */
public fun PathAggregation.revalued(mids: Map<String, String>): PathAggregation {
    if (pricingMode == PricingMode.SERVER) return this
    val newBreakdown = breakdown.map { entry ->
        when (entry.category) {
            AssetCategory.SPOT -> {
                val mid = mids[entry.asset] ?: return@map entry
                val amountDec = parseDecimalOrZero(entry.amount)
                val priceDec = parseDecimalOrNull(mid) ?: BigDecimal.ONE
                val value = amountDec * priceDec
                entry.copy(price = mid, valueUsd = decToString(value))
            }
            AssetCategory.PERP -> {
                val mid = mids[entry.asset] ?: return@map entry
                val newMid = parseDecimalOrNull(mid) ?: return@map entry
                val oldPrice = entry.price ?: return@map entry
                val oldMid = parseDecimalOrNull(oldPrice) ?: return@map entry
                if (oldMid.signum() == 0) return@map entry
                val avgEntryPrice = entry.avgEntryPrice ?: return@map entry
                val entryPrice = parseDecimalOrNull(avgEntryPrice) ?: return@map entry
                val amountDec = parseDecimalOrZero(entry.amount)
                val currentValue = parseDecimalOrZero(entry.valueUsd)
                val entryNotional = entryPrice * amountDec
                val netSignedSize = (currentValue + entryNotional).divide(oldMid, MathContext.DECIMAL128)
                val newValue = newMid * netSignedSize - entryNotional
                entry.copy(price = mid, valueUsd = decToString(newValue))
            }
            AssetCategory.EXCHANGE -> entry
        }
    }
    val totalEquity = newBreakdown.fold(BigDecimal.ZERO) { sum, entry -> sum + parseDecimalOrZero(entry.valueUsd) }
    return copy(totalEquityUsd = decToString(totalEquity), breakdown = newBreakdown)
}

/**
 * Returns a copy with `unrealizedPnl`, `returnOnEquity`, and `positionValue`
 * recomputed from the current mid price. Unknown/non-numeric mids leave the
 * position unchanged.
 */
public fun SimPosition.revalued(mids: Map<String, String>): SimPosition {
    val mid = mids[market] ?: return this
    val markDec = parseDecimalOrNull(mid) ?: return this
    val sizeDec = parseDecimalOrZero(size)
    val entryDec = parseDecimalOrZero(entryPrice)
    val signedSize = if (side == PositionSide.SHORT) sizeDec.negate() else sizeDec
    val pnl = signedSize * (markDec - entryDec)
    val posVal = sizeDec * markDec
    val marginDec = parseDecimalOrZero(marginUsed)
    val roe = if (marginDec.signum() > 0) pnl.divide(marginDec, MathContext.DECIMAL128) else BigDecimal.ZERO
    return copy(
        unrealizedPnl = decToString(pnl),
        returnOnEquity = decToString(roe),
        positionValue = decToString(posVal),
        error = null,
    )
}

/**
 * Returns a copy with `totalUnrealizedPnl`, `equity`, and `availableToWithdraw`
 * recomputed from revalued positions.
 */
internal fun SimMarginSummary.revalued(positions: List<SimPosition>): SimMarginSummary {
    val totalPnl = positions.fold(BigDecimal.ZERO) { sum, pos -> sum + parseDecimalOrZero(pos.unrealizedPnl ?: "0") }
    val rawUsd = totalRawUsd?.toBigDecimalOrNull()
    val eq = if (rawUsd != null) rawUsd + totalPnl else parseDecimalOrZero(equity)
    val maintenance = parseDecimalOrZero(maintenanceMarginRequired)
    val withdrawable = (eq - maintenance).max(BigDecimal.ZERO)
    return copy(
        equity = decToString(eq),
        availableToWithdraw = decToString(withdrawable),
        totalUnrealizedPnl = decToString(totalPnl),
    )
}

/**
 * Returns a copy with all price-derived fields recomputed from mid prices.
 * Position P&L, margin summary totals, and equity are updated. Structural data
 * (orders, account, margins, intents) is preserved unchanged.
 */
public fun ExchangeState.revalued(mids: Map<String, String>): ExchangeState {
    if (pricingMode == PricingMode.SERVER || tradingAllocation != null) return this
    val newPositions = positions.map { it.revalued(mids) }
    val newSummary = marginSummary.revalued(newPositions)
    val newCross = crossMarginSummary?.revalued(newPositions.filter { it.marginMode == MarginMode.CROSS })
    return copy(
        marginSummary = newSummary,
        crossMarginSummary = newCross,
        positions = newPositions,
    )
}

/**
 * Returns a copy of each point with `valueUsd` populated from `equityUsd`, for
 * equity-anchored P&L charts. Provides a true historical portfolio value view
 * rather than a translated P&L curve.
 */
internal fun applyEquityAnchor(points: List<PnlPoint>): List<PnlPoint> =
    points.map { it.copy(valueUsd = it.equityUsd) }
