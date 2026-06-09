package network.arca.sdk

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
}
