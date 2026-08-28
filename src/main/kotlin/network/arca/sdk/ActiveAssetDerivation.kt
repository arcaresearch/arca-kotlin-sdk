package network.arca.sdk

import network.arca.sdk.models.ActiveAssetData
import network.arca.sdk.models.AvailabilityBreakdown
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

/**
 * Perp-dex index of a canonical market id (`hl:<dexIndex>:<symbol>`), or null
 * when the id is not in the three-segment form (a bare symbol, or a venue that
 * does not carry a dex index).
 */
private fun perpDexIndexOf(market: String?): Int? {
    if (market == null) return null
    val parts = market.split(":")
    if (parts.size < 3) return null
    val idx = parts[1].toIntOrNull() ?: return null
    return if (idx >= 0) idx else null
}

/** Both buying-power numbers for an account, and which one `market` uses. */
internal data class ResolvedAvailability(
    val available: Double,
    val crossDex: Double,
    val native: Double,
    val enforced: Boolean,
    val rate: Double,
)

/**
 * An account on a venue with a cross-dex reservation has exactly TWO buying
 * power numbers, and this returns both.
 *
 * ```
 * native   = equity - totalMargin           (the venue-native dex)
 * crossDex = totalCollateral - reserved     (shared by EVERY other dex)
 * reserved = max(marginNative, rate * notionalNative) + marginOnOtherDexes
 * ```
 *
 * Pinned against Hyperliquid mainnet on 2026-08-28 across three live accounts
 * and nine constructed states. Two properties are load-bearing, and an earlier
 * revision of this logic got both wrong:
 *
 * - The notional floor applies to the VENUE-NATIVE dex only. A position on any
 *   other dex contributes exactly its own initial margin and is never charged
 *   the floor.
 * - Every non-native dex shares the ONE `crossDex` number. A position on one of
 *   them costs its margin uniformly — on the native dex, on itself, and on
 *   every sibling alike. Only a native position is asymmetric, and only above
 *   `1/rate` leverage, where the floor exceeds the margin it posted.
 *
 * The intuition: the shared pool is charged as if the native position had been
 * opened at `1/rate` leverage. Native leverage beyond that is invisible to the
 * native dex's own budget and charged in full to every other dex.
 */
internal fun resolveAvailability(
    exchangeState: ExchangeState,
    market: String,
    equity: Double,
    initialMarginUsed: Double,
): ResolvedAvailability {
    val native = maxOf(0.0, equity - initialMarginUsed)
    val model = exchangeState.collateralModel
    val rate = parsePositiveDouble(model?.crossDexReservationRate)
    val total = parsePositiveDouble(model?.totalCollateralUsd)

    // No declared rule (a single-pool venue), or a term the venue did not send:
    // there is one budget and it is the ordinary one. Never guess a reservation.
    if (model == null || !model.crossDexReservationEnforced || rate <= 0 || total <= 0) {
        return ResolvedAvailability(native, native, native, false, rate)
    }

    var marginNative = 0.0
    var notionalNative = 0.0
    var marginOtherDexes = 0.0
    for (p in exchangeState.positions) {
        val d = perpDexIndexOf(p.market) ?: continue
        val mu = parsePositiveDouble(p.marginUsed)
        if (d == 0) {
            marginNative += mu
            notionalNative += parsePositiveDouble(p.positionValue)
        } else {
            marginOtherDexes += mu
        }
    }
    val reserved = maxOf(marginNative, rate * notionalNative) + marginOtherDexes
    val crossDex = maxOf(0.0, total - reserved)
    // The native dex keeps the ordinary budget; every other dex draws on the
    // shared pool — including one this account already holds a position on,
    // because adding there still has to move collateral into it.
    val onNativeDex = perpDexIndexOf(market) == 0
    return ResolvedAvailability(
        available = if (onNativeDex) native else crossDex,
        crossDex = crossDex,
        native = native,
        enforced = !onNativeDex,
        rate = rate,
    )
}

/**
 * Both of an account's buying-power numbers, from an [ExchangeState] you
 * already have. No network call.
 *
 * [Arca.watchMaxOrderSize] reports this on every tick as
 * [ActiveAssetData.availability]; this is the same computation for callers
 * holding a one-shot `getExchangeState` result — a market list, a portfolio
 * header, or an order ticket not driving a live slider.
 *
 * ```kotlin
 * val state = arca.getExchangeState(objectId)
 * val a = marketAvailability(state, "hl:1:NVDA")
 * a.crossDexAvailableUsd // buying power on NVIDIA (and every non-native dex)
 * a.nativeAvailableUsd   // buying power on hl:0:* — larger when native is >10x
 * a.reservationEnforced  // whether THIS market draws on the shared pool
 * ```
 *
 * On a venue that declares no reservation both numbers are equal and
 * `reservationEnforced` is false, so a caller can render one code path.
 */
public fun marketAvailability(exchangeState: ExchangeState, market: String): AvailabilityBreakdown {
    val summary = exchangeState.crossMarginSummary ?: exchangeState.marginSummary
    val a = resolveAvailability(
        exchangeState,
        market,
        parsePositiveDouble(summary.equity),
        parsePositiveDouble(summary.initialMarginUsed),
    )
    return AvailabilityBreakdown(
        reservationEnforced = a.enforced,
        crossDexAvailableUsd = toDecimalString(a.crossDex),
        nativeAvailableUsd = toDecimalString(a.native),
        reservationRate = toDecimalString(a.rate),
    )
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

    // Cross bucket, not the account-wide summary. The server budgets orders from
    // cross equity alone (PositionService.AvailableBalance) because an isolated
    // position's collateral and P&L are locked to that position and can neither
    // fund nor drain another order. Deriving from `marginSummary` instead lets
    // an isolated position's unrealized profit inflate the previewed max above
    // what the venue will accept. Falls back to `marginSummary` for older
    // servers that do not send the cross bucket (identical when nothing is
    // isolated).
    val summary = exchangeState.crossMarginSummary ?: exchangeState.marginSummary
    val equity = parsePositiveDouble(summary.equity)
    val initialMarginUsed = parsePositiveDouble(summary.initialMarginUsed)
    val hasPositions = exchangeState.positions.isNotEmpty()
    val availableGuard = if (hasPositions) 0.97 else 1.0
    val avail = resolveAvailability(exchangeState, market, equity, initialMarginUsed)
    val available = maxOf(0.0, avail.available * availableGuard)
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
    // Each side splits into the part that reduces the open position and the part
    // that opens new exposure. The reduce leg is unconditional — a
    // strictly-reducing fill lowers both the initial and the maintenance
    // requirement, so the venue never refuses it for balance.
    var buyReduce = 0.0
    var buyOpen = 0.0
    var sellReduce = 0.0
    var sellOpen = 0.0

    if (currentPosition != null) {
        val posSize = parsePositiveDouble(currentPosition.size)
        // Isolated positions carry dedicated collateral that can exceed the
        // leverage-implied marginUsed after updateIsolatedMargin; closing
        // releases that full amount. Mirrors the server's lockedCollateral().
        val isolated = parsePositiveDouble(currentPosition.isolatedMargin)
        val posMargin = if (isolated > 0) isolated else parsePositiveDouble(currentPosition.marginUsed)
        val closeFees = posSize * markPx * feeRate * SAFETY_MARGIN_FACTOR
        val availableAfterClose = maxOf(0.0, available + posMargin - closeFees)

        when (currentPosition.side) {
            PositionSide.LONG -> {
                buyOpen = maxTokensForDir(available, buyPx)
                sellReduce = posSize
                sellOpen = maxTokensForDir(availableAfterClose, sellPx)
            }
            PositionSide.SHORT -> {
                sellOpen = maxTokensForDir(available, sellPx)
                buyReduce = posSize
                buyOpen = maxTokensForDir(availableAfterClose, buyPx)
            }
        }
    } else {
        buyOpen = maxTokensForDir(available, buyPx)
        sellOpen = maxTokensForDir(available, sellPx)
    }

    // Only the open legs are floored, and only because they are budget-derived:
    // floorToDecimals deliberately nudges down to avoid advertising a size the
    // venue would refuse. The reduce legs are the position size itself, already
    // at the venue's tick precision, and that nudge would shave a tick off them
    // (0.02 -> 0.01999) — leaving the user unable to fully close from a slider
    // that reads this. The server floors both with exact decimal math, where the
    // reduce leg is a no-op.
    buyOpen = floorToDecimals(buyOpen, szDecimals)
    sellOpen = floorToDecimals(sellOpen, szDecimals)
    val buyMax = buyReduce + buyOpen
    val sellMax = sellReduce + sellOpen

    val rawAvailableUsd = avail.available

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
        maxBuyReduceSize = toDecimalString(buyReduce, szDecimals),
        maxBuyOpenSize = toDecimalString(buyOpen, szDecimals),
        maxSellReduceSize = toDecimalString(sellReduce, szDecimals),
        maxSellOpenSize = toDecimalString(sellOpen, szDecimals),
        availability = AvailabilityBreakdown(
            reservationEnforced = avail.enforced,
            crossDexAvailableUsd = toDecimalString(avail.crossDex),
            nativeAvailableUsd = toDecimalString(avail.native),
            reservationRate = toDecimalString(avail.rate),
        ),
    )
}
