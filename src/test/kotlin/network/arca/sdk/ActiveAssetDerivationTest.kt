package network.arca.sdk

import network.arca.sdk.models.AvailabilityBreakdown
import network.arca.sdk.models.CollateralModel
import network.arca.sdk.models.ExchangeState
import network.arca.sdk.models.LeverageType
import network.arca.sdk.models.OrderBreakdownAmountType
import network.arca.sdk.models.OrderBreakdownOptions
import network.arca.sdk.models.OrderSide
import network.arca.sdk.models.PositionSide
import network.arca.sdk.models.SimAccount
import network.arca.sdk.models.SimFeeRates
import network.arca.sdk.models.SimMarginSummary
import network.arca.sdk.models.SimPosition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ActiveAssetDerivationTest {

    private fun makeState(
        equity: String = "10000",
        initialMarginUsed: String = "0",
        positions: List<SimPosition> = emptyList(),
        takerRate: String = "0.00035",
        platformFee: String? = "0.0001",
    ): ExchangeState = ExchangeState(
        account = SimAccount(
            id = SimAccountId("act_1"),
            realmId = RealmId("rlm_1"),
            name = "test",
            createdAt = "2026-01-01T00:00:00.000000Z",
            updatedAt = "2026-01-01T00:00:00.000000Z",
        ),
        marginSummary = SimMarginSummary(
            equity = equity,
            initialMarginUsed = initialMarginUsed,
            maintenanceMarginRequired = "0",
            availableToWithdraw = equity,
            totalNtlPos = "0",
            totalUnrealizedPnl = "0",
        ),
        positions = positions,
        feeRates = SimFeeRates(taker = takerRate, maker = "0.0001", platformFee = platformFee),
    )

    private fun makePosition(market: String, side: PositionSide, size: String, marginUsed: String): SimPosition =
        SimPosition(
            id = SimPositionId("pos_1"),
            accountId = SimAccountId("act_1"),
            realmId = RealmId("rlm_1"),
            market = market,
            side = side,
            size = size,
            entryPrice = "50000",
            leverage = 10,
            marginUsed = marginUsed,
        )

    // --- cross-dex reservation -------------------------------------------
    //
    // An account on a venue with a cross-dex reservation has exactly TWO buying
    // power numbers: the native dex's own budget, and one pool shared by every
    // other perp dex. Every case below is a reading taken from Hyperliquid
    // mainnet on 2026-08-28 — three live accounts plus states driven
    // deliberately on a test account.

    private fun dexPosition(market: String, marginUsed: String, positionValue: String): SimPosition =
        SimPosition(
            id = SimPositionId("pos_$market"),
            accountId = SimAccountId("act_1"),
            realmId = RealmId("rlm_1"),
            market = market,
            side = PositionSide.LONG,
            size = "1",
            entryPrice = "1",
            leverage = 20,
            marginUsed = marginUsed,
            positionValue = positionValue,
        )

    private fun crossDexState(
        totalCollateral: String,
        equity: String,
        initialMarginUsed: String,
        positions: List<SimPosition>,
        declaresModel: Boolean = true,
    ): ExchangeState = makeState(
        equity = equity,
        initialMarginUsed = initialMarginUsed,
        positions = positions,
    ).copy(
        collateralModel = if (declaresModel) {
            CollateralModel(
                crossDexReservationEnforced = true,
                crossDexReservationRate = "0.1",
                totalCollateralUsd = totalCollateral,
            )
        } else {
            null
        },
    )

    /** The venue's own answer for each state, on a non-native (HIP-3) market. */
    @Test
    fun `cross-dex available matches mainnet`() {
        val cases: List<Triple<String, ExchangeState, Double>> = listOf(
            // The Gobi letter tester: 40x native swallows the entire balance.
            Triple(
                "letter tester 40x",
                crossDexState("21.647938", "21.647938", "9.931551",
                    listOf(dexPosition("hl:0:BTC", "9.931551", "397.262040"))),
                0.0,
            ),
            Triple(
                "lab, headroom",
                crossDexState("9.749047", "9.749047", "3.087293",
                    listOf(dexPosition("hl:0:BTC", "3.087293", "73.436720"))),
                2.405375,
            ),
            // Two dexes — the account that falsified the previous formula.
            Triple(
                "tester2 native+xyz",
                crossDexState("8.696417", "8.668287", "4.072284",
                    listOf(dexPosition("hl:0:BTC", "2.397960", "23.979600"),
                           dexPosition("hl:1:NVDA", "1.674324", "33.486480"))),
                4.624133,
            ),
            Triple(
                "native 20x",
                crossDexState("14.753099", "14.753099", "4.994187",
                    listOf(dexPosition("hl:0:BTC", "4.994187", "99.883750"))),
                4.764724,
            ),
            // Below 1/rate the margin term wins — the branch that proves the max().
            Triple(
                "native 8x",
                crossDexState("14.700599", "14.700599", "12.479531",
                    listOf(dexPosition("hl:0:BTC", "12.479531", "99.836250"))),
                2.221068,
            ),
            // A HIP-3 position is never charged the notional floor.
            Triple(
                "HIP-3 only",
                crossDexState("14.563936", "14.563936", "0.679900",
                    listOf(dexPosition("hl:1:NVDA", "0.679900", "13.597800"))),
                13.884036,
            ),
            Triple(
                "two HIP-3 dexes",
                crossDexState("14.4957", "14.4957", "1.3362",
                    listOf(dexPosition("hl:1:NVDA", "0.6807", "13.6140"),
                           dexPosition("hl:9:US500", "0.6555", "13.1109"))),
                13.1595,
            ),
            Triple(
                "all three dexes",
                crossDexState("14.4519", "14.4519", "6.3333",
                    listOf(dexPosition("hl:0:BTC", "4.9971", "99.9425"),
                           dexPosition("hl:1:NVDA", "0.6807", "13.6134"),
                           dexPosition("hl:9:US500", "0.6555", "13.1109"))),
                3.12145,
            ),
        )
        for ((name, state, want) in cases) {
            val d = deriveActiveAssetData(state, "hl:1:NVDA", 226.59, 20, OrderSide.BUY)
            assertNotNull(d, name)
            assertEquals(want, d!!.availableToTrade.toDouble(), 0.001, name)
            assertEquals(want, d.availability!!.crossDexAvailableUsd.toDouble(), 0.001, name)
        }
    }

    /** Every non-native dex reads the SAME number, including one never touched. */
    @Test
    fun `every non-native dex shares one number`() {
        val st = crossDexState("14.4519", "14.4519", "6.3333",
            listOf(dexPosition("hl:0:BTC", "4.9971", "99.9425"),
                   dexPosition("hl:1:NVDA", "0.6807", "13.6134"),
                   dexPosition("hl:9:US500", "0.6555", "13.1109")))
        val nvda = deriveActiveAssetData(st, "hl:1:NVDA", 226.59, 20, OrderSide.BUY)!!
        val us500 = deriveActiveAssetData(st, "hl:9:US500", 771.22, 20, OrderSide.BUY)!!
        val gold = deriveActiveAssetData(st, "hl:2:GOLD", 4153.2, 20, OrderSide.BUY)!!
        assertEquals(nvda.availableToTrade, us500.availableToTrade)
        assertEquals(nvda.availableToTrade, gold.availableToTrade)
    }

    /** The native dex keeps its own, larger budget (measured 8.115 vs 3.120). */
    @Test
    fun `native dex keeps its own budget`() {
        val st = crossDexState("14.4519", "14.4519", "6.3333",
            listOf(dexPosition("hl:0:BTC", "4.9971", "99.9425"),
                   dexPosition("hl:1:NVDA", "0.6807", "13.6134"),
                   dexPosition("hl:9:US500", "0.6555", "13.1109")))
        val btc = deriveActiveAssetData(st, "hl:0:BTC", 79954.0, 20, OrderSide.BUY)!!
        assertEquals(8.1186, btc.availableToTrade.toDouble(), 0.001)
        assertEquals(false, btc.availability!!.reservationEnforced)
        assertEquals(3.12145, btc.availability!!.crossDexAvailableUsd.toDouble(), 0.001)
    }

    /** The REST path: both numbers from a one-shot state, no stream needed. */
    @Test
    fun `market availability from a plain state`() {
        val st = crossDexState("14.4519", "14.4519", "6.3333",
            listOf(dexPosition("hl:0:BTC", "4.9971", "99.9425"),
                   dexPosition("hl:1:NVDA", "0.6807", "13.6134"),
                   dexPosition("hl:9:US500", "0.6555", "13.1109")))

        val nvda = marketAvailability(st, "hl:1:NVDA")
        assertTrue(nvda.reservationEnforced)
        assertEquals(3.12145, nvda.crossDexAvailableUsd.toDouble(), 0.001)
        assertEquals(8.1186, nvda.nativeAvailableUsd.toDouble(), 0.001)

        // Same two numbers from the native market — only the flag differs.
        val btc = marketAvailability(st, "hl:0:BTC")
        assertEquals(false, btc.reservationEnforced)
        assertEquals(nvda.crossDexAvailableUsd, btc.crossDexAvailableUsd)
        assertEquals(nvda.nativeAvailableUsd, btc.nativeAvailableUsd)

        // ...and it agrees with what the stream reports for the same state.
        val streamed = deriveActiveAssetData(st, "hl:1:NVDA", 226.59, 20, OrderSide.BUY)!!
        assertEquals(nvda, streamed.availability)
    }

    /** A single-pool venue declares no model and must be left entirely alone. */
    @Test
    fun `no model means no reservation`() {
        val sim = crossDexState("14.4519", "14.4519", "6.3333",
            listOf(dexPosition("hl:0:BTC", "4.9971", "99.9425"),
                   dexPosition("hl:1:NVDA", "0.6807", "13.6134")),
            declaresModel = false)
        val d = deriveActiveAssetData(sim, "hl:1:NVDA", 226.59, 20, OrderSide.BUY)!!
        assertEquals(8.1186, d.availableToTrade.toDouble(), 0.001)
        assertEquals(false, d.availability!!.reservationEnforced)
        val a = marketAvailability(sim, "hl:1:NVDA")
        assertEquals(a.crossDexAvailableUsd, a.nativeAvailableUsd)
    }

    @Test
    fun usesEquityMinusInitialMargin_NotAvailableToWithdraw() {
        val state = ExchangeState(
            account = SimAccount(
                id = SimAccountId("act_1"),
                realmId = RealmId("rlm_1"),
                name = "test",
                createdAt = "2026-01-01T00:00:00.000000Z",
                updatedAt = "2026-01-01T00:00:00.000000Z",
            ),
            marginSummary = SimMarginSummary(
                equity = "500",
                initialMarginUsed = "400",
                maintenanceMarginRequired = "12",
                availableToWithdraw = "488",
                totalNtlPos = "10000",
                totalUnrealizedPnl = "0",
            ),
            feeRates = SimFeeRates(taker = "0.00035", maker = "0.0001", platformFee = "0.0001"),
        )

        val data = deriveActiveAssetData(state, market = "hl:0:BTC", markPx = 80000.0, leverage = 5, side = OrderSide.BUY)
        assertNotNull(data)
        val maxBuyUsd = data!!.maxBuyUsd.toDouble()
        assertTrue(maxBuyUsd < 600, "max notional ($maxBuyUsd) should be based on equity-margin (100)")
        assertTrue(maxBuyUsd > 400, "max notional ($maxBuyUsd) should be positive (~\$500 at 5x)")
    }

    @Test
    fun noPosition_SymmetricMaxSizes() {
        val state = makeState(equity = "1000")
        val data = deriveActiveAssetData(state, market = "hl:0:BTC", markPx = 50000.0, leverage = 10, side = OrderSide.BUY)
        assertNotNull(data)
        assertEquals("hl:0:BTC", data!!.market)
        assertEquals(LeverageType.CROSS, data.leverage.type)
        assertEquals(10, data.leverage.value)
        assertEquals(data.maxBuySize, data.maxSellSize, "without a position, buy and sell max should be equal")
        assertTrue(data.maxBuySize.toDouble() > 0)
    }

    @Test
    fun longPosition_SellMaxIncludesClose() {
        val pos = makePosition(market = "hl:0:BTC", side = PositionSide.LONG, size = "0.1", marginUsed = "500")
        val state = makeState(equity = "1500", initialMarginUsed = "500", positions = listOf(pos))
        val data = deriveActiveAssetData(state, market = "hl:0:BTC", markPx = 50000.0, leverage = 10, side = OrderSide.SELL)
        assertNotNull(data)
        assertTrue(
            data!!.maxSellSize.toDouble() > data.maxBuySize.toDouble(),
            "sell max should exceed buy max when long",
        )
    }

    @Test
    fun shortPosition_BuyMaxIncludesClose() {
        val pos = makePosition(market = "hl:0:BTC", side = PositionSide.SHORT, size = "0.1", marginUsed = "500")
        val state = makeState(equity = "1500", initialMarginUsed = "500", positions = listOf(pos))
        val data = deriveActiveAssetData(state, market = "hl:0:BTC", markPx = 50000.0, leverage = 10, side = OrderSide.BUY)
        assertNotNull(data)
        assertTrue(
            data!!.maxBuySize.toDouble() > data.maxSellSize.toDouble(),
            "buy max should exceed sell max when short",
        )
    }

    @Test
    fun invalidMarkPx_ReturnsNull() {
        val state = makeState()
        assertNull(deriveActiveAssetData(state, market = "hl:0:BTC", markPx = 0.0, leverage = 10, side = OrderSide.BUY))
        assertNull(deriveActiveAssetData(state, market = "hl:0:BTC", markPx = -1.0, leverage = 10, side = OrderSide.BUY))
    }

    @Test
    fun invalidLeverage_ReturnsNull() {
        val state = makeState()
        assertNull(deriveActiveAssetData(state, market = "hl:0:BTC", markPx = 50000.0, leverage = 0, side = OrderSide.BUY))
    }

    @Test
    fun zeroAvailable_ReturnsZeroMax() {
        val state = makeState(equity = "500", initialMarginUsed = "500")
        val data = deriveActiveAssetData(state, market = "hl:0:BTC", markPx = 50000.0, leverage = 10, side = OrderSide.BUY)
        assertNotNull(data)
        assertEquals("0", data!!.maxBuySize)
        assertEquals("0", data.maxSellSize)
    }

    @Test
    fun builderFeeBps_ReducesMaxSize() {
        val state = makeState(equity = "1000")
        val withoutFee = deriveActiveAssetData(state, "hl:0:BTC", 50000.0, 10, OrderSide.BUY, builderFeeBps = 0)
        val withFee = deriveActiveAssetData(state, "hl:0:BTC", 50000.0, 10, OrderSide.BUY, builderFeeBps = 100)
        assertNotNull(withoutFee)
        assertNotNull(withFee)
        assertTrue(withoutFee!!.maxBuySize.toDouble() > withFee!!.maxBuySize.toDouble())
    }

    @Test
    fun maxNotional_NeverExceedsAvailable() {
        val state = makeState(equity = "282.51")
        val data = deriveActiveAssetData(state, "hl:0:BTC", 68995.0, 1, OrderSide.SELL, szDecimals = 4)
        assertNotNull(data)
        val sellMax = data!!.maxSellSize.toDouble()
        val notional = sellMax * 68995
        assertTrue(notional <= 282.51, "max notional ($notional) must not exceed available (282.51)")
        assertTrue(sellMax > 0, "max should be positive")
    }

    @Test
    fun floorToDecimals_NoFloatingPointOvershoot() {
        val state = makeState(equity = "1000")
        var markPx = 50000.0
        while (markPx < 70000.0) {
            val data = deriveActiveAssetData(state, "hl:0:BTC", markPx, 1, OrderSide.BUY, szDecimals = 4)
            if (data != null) {
                val notional = data.maxBuySize.toDouble() * markPx
                assertTrue(notional <= 1000, "max notional ($notional) must not exceed available (1000) at markPx=$markPx")
            }
            markPx += 137.0
        }
    }

    @Test
    fun defaultPlatformFee_UsedWhenMissing() {
        val state = makeState(equity = "1000", platformFee = null)
        val data = deriveActiveAssetData(state, "hl:0:BTC", 50000.0, 10, OrderSide.BUY)
        assertNotNull(data)
        assertTrue(data!!.maxBuySize.toDouble() > 0)
    }

    @Test
    fun feeScale_ReducesMaxSize() {
        val state = makeState(equity = "1000")
        val withoutScale = deriveActiveAssetData(state, "hl:1:TSLA", 250.0, 10, OrderSide.BUY, feeScale = 1.0)
        val withScale = deriveActiveAssetData(state, "hl:1:TSLA", 250.0, 10, OrderSide.BUY, feeScale = 2.0)
        assertNotNull(withoutScale)
        assertNotNull(withScale)
        assertTrue(withoutScale!!.maxBuySize.toDouble() > withScale!!.maxBuySize.toDouble())
    }

    @Test
    fun leverage10x_200Account_YieldsApprox2KNotional() {
        val state = makeState(equity = "200")
        val data = deriveActiveAssetData(
            state, "hl:1:SILVER", 70.87, 10, OrderSide.BUY, builderFeeBps = 40, szDecimals = 5,
        )
        assertNotNull(data)
        val maxBuy = data!!.maxBuySize.toDouble()
        val maxBuyUsd = data.maxBuyUsd.toDouble()
        assertTrue(maxBuy > 25, "at 10x leverage, max buy ($maxBuy) must be well above 2.8 (1x level)")
        assertTrue(maxBuyUsd > 1800 && maxBuyUsd < 2100, "notional buying power ($maxBuyUsd) should be ~\$2,000")
        assertEquals(10, data.leverage.value)
    }

    @Test
    fun feeScale_DefaultsToOne() {
        val state = makeState(equity = "1000")
        val explicit = deriveActiveAssetData(state, "hl:0:BTC", 50000.0, 10, OrderSide.BUY, feeScale = 1.0)
        val implicit = deriveActiveAssetData(state, "hl:0:BTC", 50000.0, 10, OrderSide.BUY)
        assertNotNull(explicit)
        assertNotNull(implicit)
        assertEquals(explicit!!.maxBuySize, implicit!!.maxBuySize, "omitting feeScale should behave like feeScale=1")
    }

    // MARK: - orderBreakdown tests

    @Test
    fun orderBreakdown_SpendMode() {
        val result = Arca.orderBreakdown(
            OrderBreakdownOptions(
                amount = "200", amountType = OrderBreakdownAmountType.SPEND, leverage = 10,
                feeRate = "0.00045", price = "70.87", side = OrderSide.BUY, szDecimals = 5,
            ),
        )
        assertTrue(kotlin.math.abs(result.totalSpend.toDouble() - 200) < 1)
        val notional = result.notionalUsd.toDouble()
        assertTrue(notional > 1900 && notional < 2000, "notional ($notional) should be ~1991")
        assertTrue(result.tokens.toDouble() > 0)
        assertEquals("70.87", result.price)
        assertEquals("0.00045", result.feeRate)
    }

    @Test
    fun orderBreakdown_NotionalMode() {
        val result = Arca.orderBreakdown(
            OrderBreakdownOptions(
                amount = "2000", amountType = OrderBreakdownAmountType.NOTIONAL, leverage = 10,
                feeRate = "0.00045", price = "100", side = OrderSide.SELL, szDecimals = 3,
            ),
        )
        assertEquals(20.0, result.tokens.toDouble(), 0.001)
        assertEquals(2000.0, result.notionalUsd.toDouble(), 0.01)
        assertEquals(200.0, result.marginRequired.toDouble(), 0.01)
        assertTrue(kotlin.math.abs(result.estimatedFee.toDouble() - 0.9) < 0.1)
    }

    @Test
    fun orderBreakdown_TokensMode() {
        val result = Arca.orderBreakdown(
            OrderBreakdownOptions(
                amount = "5", amountType = OrderBreakdownAmountType.TOKENS, leverage = 2,
                feeRate = "0.001", price = "50", side = OrderSide.BUY, szDecimals = 2,
            ),
        )
        assertEquals(5.0, result.tokens.toDouble(), 0.01)
        assertEquals(250.0, result.notionalUsd.toDouble(), 0.01)
        assertEquals(125.0, result.marginRequired.toDouble(), 0.01)
        assertEquals(0.25, result.estimatedFee.toDouble(), 0.001)
        assertEquals(125.25, result.totalSpend.toDouble(), 0.01)
    }

    @Test
    fun orderBreakdown_ZeroAmount_ReturnsZeros() {
        val result = Arca.orderBreakdown(
            OrderBreakdownOptions(
                amount = "0", amountType = OrderBreakdownAmountType.SPEND, leverage = 10,
                feeRate = "0.001", price = "100", side = OrderSide.BUY,
            ),
        )
        assertEquals("0", result.tokens)
        assertEquals("0", result.totalSpend)
    }

    // MARK: - Maintenance margin rate

    @Test
    fun threadsMaintenanceMarginRateThrough() {
        val derived = deriveActiveAssetData(
            makeState(), market = "BTC", markPx = 80000.0, leverage = 5, side = OrderSide.BUY,
            maintenanceMarginRate = "0.01",
        )
        assertNotNull(derived)
        assertEquals("0.01", derived!!.maintenanceMarginRate)
    }

    @Test
    fun defaultsMaintenanceMarginRateTo003WhenOmitted() {
        val derived = deriveActiveAssetData(makeState(), market = "BTC", markPx = 80000.0, leverage = 5, side = OrderSide.BUY)
        assertNotNull(derived)
        assertEquals("0.03", derived!!.maintenanceMarginRate)
    }

    // MARK: - Directional spread pricing

    @Test
    fun askRatioShrinksMaxBuySize() {
        val state = makeState(equity = "10000")
        val mid = deriveActiveAssetData(state, "hl:0:BTC", 80000.0, 5, OrderSide.BUY)
        val askAware = deriveActiveAssetData(state, "hl:0:BTC", 80000.0, 5, OrderSide.BUY, askRatio = 1.001, bidRatio = 1.0)
        assertNotNull(mid)
        assertNotNull(askAware)
        assertTrue(askAware!!.maxBuySize.toDouble() < mid!!.maxBuySize.toDouble())
        val ratio = askAware.maxBuySize.toDouble() / mid.maxBuySize.toDouble()
        assertTrue(ratio > 0.995)
        assertTrue(ratio < 1)
        assertEquals(80000 * 1.001, askAware.askPx!!.toDouble(), 1.0)
    }

    @Test
    fun bidRatioGrowsMaxSellSizeTowardServerParity() {
        val state = makeState(equity = "10000")
        val mid = deriveActiveAssetData(state, "hl:0:BTC", 80000.0, 5, OrderSide.SELL)
        val bidAware = deriveActiveAssetData(state, "hl:0:BTC", 80000.0, 5, OrderSide.SELL, askRatio = 1.0, bidRatio = 0.999)
        assertNotNull(mid)
        assertNotNull(bidAware)
        assertTrue(bidAware!!.maxSellSize.toDouble() > mid!!.maxSellSize.toDouble())
        assertEquals(80000 * 0.999, bidAware.bidPx!!.toDouble(), 1.0)
    }

    @Test
    fun defaultRatiosReproduceMidBasedSizing() {
        val state = makeState(equity = "10000")
        val explicit = deriveActiveAssetData(state, "hl:0:BTC", 80000.0, 5, OrderSide.BUY, askRatio = 1.0, bidRatio = 1.0)
        val implicit = deriveActiveAssetData(state, "hl:0:BTC", 80000.0, 5, OrderSide.BUY)
        assertNotNull(explicit)
        assertNotNull(implicit)
        assertEquals(explicit!!.maxBuySize, implicit!!.maxBuySize)
        assertEquals(explicit.maxSellSize, implicit.maxSellSize)
        assertEquals(explicit.bidPx, implicit.markPx)
        assertEquals(explicit.askPx, implicit.markPx)
    }

    @Test
    fun ignoresNonPositiveRatios() {
        val state = makeState(equity = "10000")
        val bad = deriveActiveAssetData(state, "hl:0:BTC", 80000.0, 5, OrderSide.BUY, askRatio = 0.0, bidRatio = Double.NaN)
        val mid = deriveActiveAssetData(state, "hl:0:BTC", 80000.0, 5, OrderSide.BUY)
        assertNotNull(bad)
        assertNotNull(mid)
        assertEquals(bad!!.maxBuySize, mid!!.maxBuySize)
    }

    @Test
    fun spreadRatioAppliesToLiveMidNotSnapshotMid() {
        val state = makeState(equity = "10000")
        val askRatio = 1.002
        val atSnapshot = deriveActiveAssetData(state, "hl:0:BTC", 80000.0, 5, OrderSide.BUY, askRatio = askRatio, bidRatio = 1.0)
        val afterMove = deriveActiveAssetData(state, "hl:0:BTC", 90000.0, 5, OrderSide.BUY, askRatio = askRatio, bidRatio = 1.0)
        assertNotNull(atSnapshot)
        assertNotNull(afterMove)
        assertEquals(80000 * askRatio, atSnapshot!!.askPx!!.toDouble(), 1.0)
        assertEquals(90000 * askRatio, afterMove!!.askPx!!.toDouble(), 1.0)
    }

    // --- Reduce / open split ---

    @Test
    fun noPositionReportsZeroReduceOnBothSides() {
        val d = deriveActiveAssetData(makeState(equity = "10000"), "hl:0:BTC", 80000.0, 5, OrderSide.BUY)
        assertNotNull(d)
        assertEquals("0", d!!.maxBuyReduceSize)
        assertEquals("0", d.maxSellReduceSize)
        assertEquals(d.maxBuySize, d.maxBuyOpenSize)
        assertEquals(d.maxSellSize, d.maxSellOpenSize)
    }

    @Test
    fun longPositionSplitsTheSellSideOnly() {
        val pos = makePosition("hl:0:BTC", PositionSide.LONG, "0.02", "320")
        val state = makeState(equity = "10000", initialMarginUsed = "320", positions = listOf(pos))
        val d = deriveActiveAssetData(state, "hl:0:BTC", 80000.0, 5, OrderSide.SELL)
        assertNotNull(d)
        // A sell closes the long, so the whole position is reducible. The tick
        // must survive: 0.02, not 0.01999 — a user has to be able to fully close.
        assertEquals(0.02, d!!.maxSellReduceSize!!.toDouble(), 1e-12)
        assertTrue(d.maxSellOpenSize!!.toDouble() > 0)
        // A buy adds to the long; there is nothing on that side to reduce.
        assertEquals("0", d.maxBuyReduceSize)
        assertEquals(d.maxBuySize, d.maxBuyOpenSize)
    }

    @Test
    fun totalEqualsReducePlusOpen() {
        val pos = makePosition("hl:0:BTC", PositionSide.LONG, "0.02", "320")
        val state = makeState(equity = "10000", initialMarginUsed = "320", positions = listOf(pos))
        val d = deriveActiveAssetData(state, "hl:0:BTC", 80000.0, 5, OrderSide.SELL)
        assertNotNull(d)
        assertEquals(
            d!!.maxSellReduceSize!!.toDouble() + d.maxSellOpenSize!!.toDouble(),
            d.maxSellSize.toDouble(), 1e-10,
        )
        assertEquals(
            d.maxBuyReduceSize!!.toDouble() + d.maxBuyOpenSize!!.toDouble(),
            d.maxBuySize.toDouble(), 1e-10,
        )
    }

    // The reported bug in its client-side form: an account below its
    // initial-margin requirement can open nothing, but it can always close what
    // it holds. A slider reading only the total must still offer the trim.
    @Test
    fun reduceLegSurvivesWhenNothingCanBeOpened() {
        val pos = makePosition("hl:0:BTC", PositionSide.LONG, "0.5", "4000")
        val state = makeState(equity = "100", initialMarginUsed = "5000", positions = listOf(pos))
        val d = deriveActiveAssetData(state, "hl:0:BTC", 80000.0, 5, OrderSide.SELL)
        assertNotNull(d)
        assertEquals(0.5, d!!.maxSellReduceSize!!.toDouble(), 1e-12)
        assertTrue(d.maxSellSize.toDouble() >= 0.5)
    }

    // --- Isolated positions ---

    // Isolated collateral and P&L are locked to their own position: the server
    // budgets orders from cross equity alone (PositionService.AvailableBalance).
    // Deriving from the account-wide summary let an isolated position's profit
    // inflate the previewed max above what the venue would accept.
    @Test
    fun prefersCrossBucketOverAccountWideSummary() {
        val state = ExchangeState(
            account = SimAccount(
                id = SimAccountId("act_1"),
                realmId = RealmId("rlm_1"),
                name = "test",
                createdAt = "2026-01-01T00:00:00.000000Z",
                updatedAt = "2026-01-01T00:00:00.000000Z",
            ),
            // Account-wide: $9,000 free, most of it locked inside an isolated position.
            marginSummary = SimMarginSummary(
                equity = "10000",
                initialMarginUsed = "1000",
                maintenanceMarginRequired = "0",
                availableToWithdraw = "9000",
                totalNtlPos = "0",
                totalUnrealizedPnl = "0",
            ),
            // Cross bucket: only $500 is actually spendable.
            crossMarginSummary = SimMarginSummary(
                equity = "1500",
                initialMarginUsed = "1000",
                maintenanceMarginRequired = "0",
                availableToWithdraw = "500",
                totalNtlPos = "0",
                totalUnrealizedPnl = "0",
            ),
            feeRates = SimFeeRates(taker = "0.00035", maker = "0.0001", platformFee = "0.0001"),
        )

        val d = deriveActiveAssetData(state, "hl:0:BTC", 80000.0, 5, OrderSide.BUY)
        assertNotNull(d)
        // $500 of cross collateral at 5x is ~$2.5k of notional, not the ~$45k
        // the account-wide summary would have implied.
        assertTrue(d!!.maxBuyUsd.toDouble() < 3000, "derived ${d.maxBuyUsd} from the wrong bucket")
        assertEquals(500.0, d.availableToTrade.toDouble(), 1e-6)
    }

    @Test
    fun fallsBackToAccountWideSummaryWhenNoCrossBucket() {
        // Older servers, and any account with nothing isolated, where the two
        // are identical by construction.
        val d = deriveActiveAssetData(makeState(equity = "1000"), "hl:0:BTC", 80000.0, 5, OrderSide.BUY)
        assertNotNull(d)
        assertEquals(1000.0, d!!.availableToTrade.toDouble(), 1e-6)
    }

    // An isolated position can hold more collateral than its leverage implies
    // after updateIsolatedMargin; closing releases the whole amount. Mirrors the
    // server's lockedCollateral().
    @Test
    fun releasesIsolatedMarginNotMarginUsedIntoTheReversingBudget() {
        val plainPos = makePosition("hl:0:BTC", PositionSide.LONG, "0.02", "320")
        val topUpPos = plainPos.copy(isolatedMargin = "2000")

        val topUp = deriveActiveAssetData(
            makeState(equity = "1000", initialMarginUsed = "320", positions = listOf(topUpPos)),
            "hl:0:BTC", 80000.0, 5, OrderSide.SELL,
        )
        val plain = deriveActiveAssetData(
            makeState(equity = "1000", initialMarginUsed = "320", positions = listOf(plainPos)),
            "hl:0:BTC", 80000.0, 5, OrderSide.SELL,
        )
        assertNotNull(topUp)
        assertNotNull(plain)
        // The extra $1,680 of dedicated collateral is released by the close and
        // is spendable on the reversing leg, so the open portion grows.
        assertTrue(topUp!!.maxSellOpenSize!!.toDouble() > plain!!.maxSellOpenSize!!.toDouble())
        // The reduce leg is the position either way — collateral doesn't change it.
        assertEquals(topUp.maxSellReduceSize, plain.maxSellReduceSize)
    }
}
