package network.arca.sdk

import network.arca.sdk.models.ActiveAssetData
import network.arca.sdk.models.ExchangeState
import network.arca.sdk.models.LeverageInfo
import network.arca.sdk.models.LeverageType
import network.arca.sdk.models.MarginTier
import network.arca.sdk.models.OrderSide
import network.arca.sdk.models.PositionSide
import java.util.Locale
import kotlin.math.floor
import kotlin.math.pow

private const val SAFETY_MARGIN_FACTOR: Double = 1.001 // 10 bps multiplicative buffer on total cost

// The Arca network takes no platform fee. When the server omits a platformFee
// rate we assume 0; the server reports the live rate in feeRates.platformFee
// if a network fee is ever re-enabled.
private const val DEFAULT_PLATFORM_FEE_RATE: Double = 0.0

private fun parsePositiveDouble(value: String?): Double {
    val n = value?.toDoubleOrNull() ?: return 0.0
    return if (n.isFinite() && n > 0) n else 0.0
}

private fun floorToDecimals(value: Double, decimals: Int): Double {
    if (!value.isFinite() || value <= 0) return 0.0
    val factor = 10.0.pow(decimals.toDouble())
    // IEEE 754: division can land epsilon above a tick boundary, e.g.
    // 0.004099... becomes 0.00410000000000001, making floor(x * 10000) = 41
    // instead of 40. Nudge down by 1e-9 before flooring to prevent overshoot.
    return maxOf(0.0, floor(value * factor - 1e-9)) / factor
}

private fun toDecimalString(value: Double, decimals: Int = 8): String {
    if (!value.isFinite()) return "0"
    var s = String.format(Locale.US, "%.${decimals}f", value)
    if (s.contains(".")) {
        while (s.endsWith("0")) s = s.dropLast(1)
        if (s.endsWith(".")) s = s.dropLast(1)
    }
    return s
}

/**
 * Derives [ActiveAssetData] from an [ExchangeState] and user-selected trading
 * parameters, matching the TypeScript/Swift SDKs'
 * `deriveActiveAssetDataFromState` implementation.
 */
public fun deriveActiveAssetData(
    exchangeState: ExchangeState,
    market: String,
    markPx: Double,
    leverage: Int,
    @Suppress("UNUSED_PARAMETER") side: OrderSide,
    builderFeeBps: Int = 0,
    szDecimals: Int = 5,
    feeScale: Double = 1.0,
    maintenanceMarginRate: String? = null,
    marginTiers: List<MarginTier>? = null,
    askRatio: Double = 1.0,
    bidRatio: Double = 1.0,
): ActiveAssetData? {
    if (!markPx.isFinite() || markPx <= 0 || leverage <= 0) return null

    val equity = parsePositiveDouble(exchangeState.marginSummary.equity)
    val initialMarginUsed = parsePositiveDouble(exchangeState.marginSummary.initialMarginUsed)
    val hasPositions = exchangeState.positions.isNotEmpty()
    val availableGuard = if (hasPositions) 0.97 else 1.0
    val available = maxOf(0.0, (equity - initialMarginUsed) * availableGuard)
    val takerRate = parsePositiveDouble(exchangeState.feeRates?.taker)
    val effectiveScale = if (feeScale.isFinite() && feeScale > 0) feeScale else 1.0
    val platformRate = run {
        val parsed = parsePositiveDouble(exchangeState.feeRates?.platformFee)
        if (parsed > 0) parsed else DEFAULT_PLATFORM_FEE_RATE
    }
    val builderRate = if (builderFeeBps > 0) builderFeeBps.toDouble() / 100_000 else 0.0
    val feeRate = takerRate * effectiveScale + platformRate + builderRate

    // Directional execution prices. Max notional is price-independent; the price
    // only enters when converting notional -> tokens. Buys execute at the ask,
    // sells at the bid.
    val safeAskRatio = if (askRatio.isFinite() && askRatio > 0) askRatio else 1.0
    val safeBidRatio = if (bidRatio.isFinite() && bidRatio > 0) bidRatio else 1.0
    val buyPx = markPx * safeAskRatio
    val sellPx = markPx * safeBidRatio

    fun maxTokensForDir(avail: Double, execPx: Double): Double {
        if (!avail.isFinite() || avail <= 0) return 0.0
        val targetSpend = avail / SAFETY_MARGIN_FACTOR

        var activeRate = 1.0 / leverage.toDouble()
        var deduction = 0.0

        val tiers = marginTiers
        if (tiers != null && tiers.isNotEmpty()) {
            val tierMaxLev = tiers[0].maxLeverage
            var effLev = leverage
            if (tierMaxLev < effLev) effLev = tierMaxLev
            activeRate = 1.0 / effLev.toDouble()
            var prevRate = activeRate
            var prevDeduction = 0.0

            for (tier in tiers) {
                val lowerBound = tier.lowerBound.toDoubleOrNull() ?: continue
                val tierLev = tier.maxLeverage
                var lev = leverage
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
        }

        val notional = (targetSpend + deduction) / (activeRate + feeRate)
        if (!notional.isFinite() || notional <= 0) return 0.0
        return floorToDecimals(notional / execPx, szDecimals)
    }

    val currentPosition = exchangeState.positions.firstOrNull { it.market == market }
    var buyMax = 0.0
    var sellMax = 0.0

    if (currentPosition != null) {
        val posSize = parsePositiveDouble(currentPosition.size)
        val posMargin = parsePositiveDouble(currentPosition.marginUsed)
        val closeFees = posSize * markPx * feeRate * SAFETY_MARGIN_FACTOR
        val availableAfterClose = maxOf(0.0, available + posMargin - closeFees)

        when (currentPosition.side) {
            PositionSide.LONG -> {
                buyMax = maxTokensForDir(available, buyPx)
                sellMax = posSize + maxTokensForDir(availableAfterClose, sellPx)
            }
            PositionSide.SHORT -> {
                sellMax = maxTokensForDir(available, sellPx)
                buyMax = posSize + maxTokensForDir(availableAfterClose, buyPx)
            }
        }
    } else {
        buyMax = maxTokensForDir(available, buyPx)
        sellMax = maxTokensForDir(available, sellPx)
    }

    buyMax = floorToDecimals(buyMax, szDecimals)
    sellMax = floorToDecimals(sellMax, szDecimals)

    val rawAvailableUsd = maxOf(0.0, equity - initialMarginUsed)

    return ActiveAssetData(
        market = market,
        leverage = LeverageInfo(type = LeverageType.CROSS, value = leverage),
        maxBuySize = toDecimalString(buyMax, szDecimals),
        maxSellSize = toDecimalString(sellMax, szDecimals),
        maxBuyUsd = toDecimalString(buyMax * markPx),
        maxSellUsd = toDecimalString(sellMax * markPx),
        availableToTrade = toDecimalString(rawAvailableUsd),
        markPx = toDecimalString(markPx),
        feeRate = toDecimalString(feeRate),
        maintenanceMarginRate = maintenanceMarginRate ?: "0.03",
        marginTiers = marginTiers,
        bidPx = toDecimalString(sellPx),
        askPx = toDecimalString(buyPx),
    )
}
