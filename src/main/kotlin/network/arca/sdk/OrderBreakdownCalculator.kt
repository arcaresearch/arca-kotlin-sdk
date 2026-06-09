package network.arca.sdk

import network.arca.sdk.models.OrderBreakdown
import network.arca.sdk.models.OrderBreakdownAmountType
import network.arca.sdk.models.OrderBreakdownExistingPosition
import network.arca.sdk.models.OrderBreakdownOptions
import network.arca.sdk.models.OrderSide
import network.arca.sdk.models.PositionSide
import java.util.Locale
import kotlin.math.floor
import kotlin.math.pow

private fun fmt(v: Double, d: Int = 8): String {
    if (!v.isFinite()) return "0"
    var s = String.format(Locale.US, "%.${d}f", v)
    if (s.contains(".")) {
        while (s.endsWith("0")) s = s.dropLast(1)
        if (s.endsWith(".")) s = s.dropLast(1)
    }
    return s
}

private data class MergedPosition(val size: Double, val entry: Double, val side: PositionSide)

/**
 * Merge a hypothetical fill with an existing same-coin position using the same
 * rules as `PositionService.ApplyFill` in sim-exchange-go. Returns the resulting
 * (size, entry, side) triple, or `null` when the fill would fully close the
 * existing position.
 */
private fun mergeOrderWithPosition(
    newSide: PositionSide,
    newSize: Double,
    fillPrice: Double,
    existing: OrderBreakdownExistingPosition?,
): MergedPosition? {
    if (!newSize.isFinite() || newSize <= 0 || !fillPrice.isFinite() || fillPrice <= 0) return null
    val exSize = existing?.size?.toDoubleOrNull()
    val exEntry = existing?.entryPrice?.toDoubleOrNull()
    if (existing == null || exSize == null || !exSize.isFinite() || exSize <= 0 ||
        exEntry == null || !exEntry.isFinite() || exEntry <= 0
    ) {
        return MergedPosition(newSize, fillPrice, newSide)
    }

    if (existing.side == newSide) {
        val mergedSize = exSize + newSize
        val mergedEntry = (exSize * exEntry + newSize * fillPrice) / mergedSize
        return MergedPosition(mergedSize, mergedEntry, newSide)
    }
    if (newSize < exSize) {
        return MergedPosition(exSize - newSize, exEntry, existing.side)
    }
    if (newSize == exSize) {
        return null
    }
    return MergedPosition(newSize - exSize, fillPrice, newSide)
}

/**
 * Pure calculator that converts between spend (gross), notional (net), and
 * token representations of an order. No network call.
 */
internal fun computeOrderBreakdown(opts: OrderBreakdownOptions): OrderBreakdown {
    val price = opts.price.toDoubleOrNull() ?: 0.0
    val feeRate = opts.feeRate.toDoubleOrNull() ?: 0.0
    val leverage = opts.leverage.toDouble()
    val amount = opts.amount.toDoubleOrNull() ?: 0.0
    val szDecimals = opts.szDecimals

    val zero = OrderBreakdown(
        tokens = "0", notionalUsd = "0", marginRequired = "0",
        estimatedFee = "0", totalSpend = "0", price = opts.price, feeRate = opts.feeRate,
        estimatedLiquidationPrice = null,
    )
    if (price <= 0 || leverage <= 0 || feeRate < 0 || amount <= 0) return zero

    val tiers = opts.marginTiers

    val notional: Double = when (opts.amountType) {
        OrderBreakdownAmountType.SPEND -> {
            if (tiers != null && tiers.isNotEmpty()) {
                val targetSpend = amount
                var deduction = 0.0
                val tierMaxLev = tiers[0].maxLeverage
                var effLev = leverage.toInt()
                if (tierMaxLev < effLev) effLev = tierMaxLev
                var activeRate = 1.0 / effLev.toDouble()
                var prevRate = activeRate
                var prevDeduction = 0.0

                for (tier in tiers) {
                    val lowerBound = tier.lowerBound.toDoubleOrNull() ?: continue
                    val tierLev = tier.maxLeverage
                    var lev = leverage.toInt()
                    if (tierLev < lev) lev = tierLev
                    val rate = 1.0 / lev.toDouble()

                    val nextDeduction = prevDeduction + lowerBound * (rate - prevRate)
                    val spendAtBound = lowerBound * rate - nextDeduction + lowerBound * feeRate

                    if (targetSpend < spendAtBound) break

                    activeRate = rate
                    prevRate = rate
                    prevDeduction = nextDeduction
                    deduction = nextDeduction
                }
                (targetSpend + deduction) / (activeRate + feeRate)
            } else {
                amount / (1 / leverage + feeRate)
            }
        }
        OrderBreakdownAmountType.NOTIONAL -> amount
        OrderBreakdownAmountType.TOKENS -> amount * price
    }

    val factor = 10.0.pow(szDecimals.toDouble())
    val tokens = floor(notional / price * factor) / factor
    val actualNotional = tokens * price

    var marginRequiredNum = 0.0
    var nextTierThresholdNum: Double? = null
    var mmRequiredNum: Double? = null

    if (tiers != null && tiers.isNotEmpty()) {
        var deduction = 0.0
        val tierMaxLev = tiers[0].maxLeverage
        var effLev = leverage.toInt()
        if (tierMaxLev < effLev) effLev = tierMaxLev
        var activeRate = 1.0 / effLev.toDouble()
        var prevRate = activeRate
        var prevDeduction = 0.0

        for (tier in tiers) {
            val lowerBound = tier.lowerBound.toDoubleOrNull() ?: continue
            val tierLev = tier.maxLeverage
            var lev = leverage.toInt()
            if (tierLev < lev) lev = tierLev
            val rate = 1.0 / lev.toDouble()

            if (actualNotional < lowerBound) {
                nextTierThresholdNum = lowerBound
                break
            }

            deduction = prevDeduction + lowerBound * (rate - prevRate)

            activeRate = rate
            prevRate = rate
            prevDeduction = deduction
        }
        marginRequiredNum = actualNotional * activeRate - deduction

        var mmDeduction = 0.0
        var mmActiveRate = 0.5 / tiers[0].maxLeverage.toDouble()
        var mmPrevRate = mmActiveRate
        var mmPrevDeduction = 0.0

        for (tier in tiers) {
            val lowerBound = tier.lowerBound.toDoubleOrNull() ?: continue
            val rate = 0.5 / tier.maxLeverage.toDouble()

            if (actualNotional < lowerBound) break

            mmDeduction = mmPrevDeduction + lowerBound * (rate - mmPrevRate)

            mmActiveRate = rate
            mmPrevRate = rate
            mmPrevDeduction = mmDeduction
        }
        mmRequiredNum = actualNotional * mmActiveRate - mmDeduction
    } else {
        marginRequiredNum = actualNotional / leverage
    }

    val estimatedFee = actualNotional * feeRate
    val totalSpend = marginRequiredNum + estimatedFee

    var estimatedLiquidationPrice: String? = null
    val ctx = opts.accountContext
    val equity = ctx?.equity?.toDoubleOrNull()
    val otherMM = ctx?.otherMaintenanceMargin?.toDoubleOrNull()
    if ((opts.maintenanceMarginRate != null || opts.marginTiers != null) &&
        ctx != null && equity != null && equity.isFinite() && otherMM != null && otherMM.isFinite()
    ) {
        val newSide = if (opts.side == OrderSide.BUY) PositionSide.LONG else PositionSide.SHORT
        val merged = mergeOrderWithPosition(newSide, tokens, price, ctx.existingPosition)
        if (merged != null && merged.size > 0) {
            val mergedNotional = merged.size * merged.entry
            var mmMerged = 0.0

            if (tiers != null && tiers.isNotEmpty()) {
                var mmDeduction = 0.0
                var mmActiveRate = 0.5 / tiers[0].maxLeverage.toDouble()
                var mmPrevRate = mmActiveRate
                var mmPrevDeduction = 0.0

                for (tier in tiers) {
                    val lowerBound = tier.lowerBound.toDoubleOrNull() ?: continue
                    val rate = 0.5 / tier.maxLeverage.toDouble()

                    if (mergedNotional < lowerBound) break

                    mmDeduction = mmPrevDeduction + lowerBound * (rate - mmPrevRate)

                    mmActiveRate = rate
                    mmPrevRate = rate
                    mmPrevDeduction = mmDeduction
                }
                mmMerged = mergedNotional * mmActiveRate - mmDeduction
            } else {
                val mmr = opts.maintenanceMarginRate?.toDoubleOrNull()
                if (mmr != null && mmr >= 0 && mmr.isFinite()) {
                    mmMerged = mmr * mergedNotional
                }
            }

            val equityPost = equity - estimatedFee
            val marginAvail = equityPost - (otherMM + mmMerged)
            if (marginAvail > 0) {
                val perUnit = marginAvail / merged.size
                val liq = if (merged.side == PositionSide.LONG) price - perUnit else price + perUnit
                if (liq > 0) {
                    estimatedLiquidationPrice = fmt(liq)
                }
            }
        }
    }

    return OrderBreakdown(
        tokens = fmt(tokens, szDecimals),
        notionalUsd = fmt(actualNotional),
        marginRequired = fmt(marginRequiredNum),
        estimatedFee = fmt(estimatedFee),
        totalSpend = fmt(totalSpend),
        price = opts.price,
        feeRate = opts.feeRate,
        effectiveLeverage = if (opts.marginTiers != null) {
            if (actualNotional > 0) fmt(actualNotional / marginRequiredNum) else fmt(leverage)
        } else {
            null
        },
        effectiveMaintenanceMarginRate = if (opts.marginTiers != null && mmRequiredNum != null && actualNotional > 0) {
            fmt(mmRequiredNum / actualNotional)
        } else {
            null
        },
        nextTierThreshold = nextTierThresholdNum?.let { fmt(it) },
        estimatedLiquidationPrice = estimatedLiquidationPrice,
    )
}

/**
 * Validate that a path argument starts with "/". Paths without a trailing slash
 * target a single object; paths with a trailing slash aggregate all objects
 * under that prefix.
 */
internal fun validatePath(path: String) {
    if (!path.startsWith("/")) {
        throw ArcaException.Validation(
            message = "Path must start with '/'. Got: \"$path\". " +
                "Use a trailing slash for aggregation (e.g., '/users/alice/') " +
                "or an exact path for a single object (e.g., '/users/alice/main').",
            errorId = null,
        )
    }
}
