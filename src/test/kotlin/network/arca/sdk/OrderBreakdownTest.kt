package network.arca.sdk

import network.arca.sdk.models.OrderBreakdownAccountContext
import network.arca.sdk.models.OrderBreakdownAmountType
import network.arca.sdk.models.OrderBreakdownExistingPosition
import network.arca.sdk.models.OrderBreakdownOptions
import network.arca.sdk.models.OrderSide
import network.arca.sdk.models.PositionSide
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class OrderBreakdownTest {

    @Test
    fun spendMode() {
        val opts = OrderBreakdownOptions(
            amount = "200",
            amountType = OrderBreakdownAmountType.SPEND,
            leverage = 10,
            feeRate = "0.00045",
            price = "70.87",
            side = OrderSide.BUY,
            szDecimals = 5,
        )
        val result = Arca.orderBreakdown(opts)

        assertEquals("70.87", result.price)
        assertEquals("0.00045", result.feeRate)
        assertTrue(result.totalSpend.toDouble() > 199.9 && result.totalSpend.toDouble() <= 200.1)
        assertTrue(result.notionalUsd.toDouble() > 1990 && result.notionalUsd.toDouble() < 1995)
        assertTrue(result.marginRequired.toDouble() > 199 && result.marginRequired.toDouble() < 199.5)
        assertTrue(result.estimatedFee.toDouble() > 0.8 && result.estimatedFee.toDouble() < 1.0)
        assertTrue(result.tokens.toDouble() > 0)
        assertNull(result.estimatedLiquidationPrice)
    }

    @Test
    fun crossMarginLiqLongNoOtherPositions() {
        val opts = OrderBreakdownOptions(
            amount = "10", amountType = OrderBreakdownAmountType.TOKENS, leverage = 10,
            feeRate = "0.00045", price = "50000", side = OrderSide.BUY, szDecimals = 5,
            maintenanceMarginRate = "0.03",
            accountContext = OrderBreakdownAccountContext(equity = "50225", otherMaintenanceMargin = "0"),
        )
        val result = Arca.orderBreakdown(opts)
        assertEquals(46500.0, result.estimatedLiquidationPrice!!.toDouble(), 0.5)
    }

    @Test
    fun crossMarginLiqShortNoOtherPositions() {
        val opts = OrderBreakdownOptions(
            amount = "10", amountType = OrderBreakdownAmountType.TOKENS, leverage = 10,
            feeRate = "0.00045", price = "50000", side = OrderSide.SELL, szDecimals = 5,
            maintenanceMarginRate = "0.03",
            accountContext = OrderBreakdownAccountContext(equity = "50225", otherMaintenanceMargin = "0"),
        )
        val result = Arca.orderBreakdown(opts)
        assertEquals(53500.0, result.estimatedLiquidationPrice!!.toDouble(), 0.5)
    }

    @Test
    fun crossMarginLiqAccountsForOtherPositionsMM() {
        val opts = OrderBreakdownOptions(
            amount = "10", amountType = OrderBreakdownAmountType.TOKENS, leverage = 10,
            feeRate = "0.00045", price = "50000", side = OrderSide.BUY, szDecimals = 5,
            maintenanceMarginRate = "0.03",
            accountContext = OrderBreakdownAccountContext(equity = "60000", otherMaintenanceMargin = "5000"),
        )
        val result = Arca.orderBreakdown(opts)
        assertEquals(46022.5, result.estimatedLiquidationPrice!!.toDouble(), 0.5)
    }

    @Test
    fun sameSideMergeBlendsEntryPrice() {
        val opts = OrderBreakdownOptions(
            amount = "10", amountType = OrderBreakdownAmountType.TOKENS, leverage = 10,
            feeRate = "0.00045", price = "50000", side = OrderSide.BUY, szDecimals = 5,
            maintenanceMarginRate = "0.03",
            accountContext = OrderBreakdownAccountContext(
                equity = "100000", otherMaintenanceMargin = "0",
                existingPosition = OrderBreakdownExistingPosition(side = PositionSide.LONG, size = "5", entryPrice = "40000"),
            ),
        )
        val result = Arca.orderBreakdown(opts)
        assertEquals(44748.333, result.estimatedLiquidationPrice!!.toDouble(), 1.0)
    }

    @Test
    fun oppositeSideReduceKeepsExistingEntry() {
        val opts = OrderBreakdownOptions(
            amount = "4", amountType = OrderBreakdownAmountType.TOKENS, leverage = 5,
            feeRate = "0.00045", price = "50000", side = OrderSide.SELL, szDecimals = 5,
            maintenanceMarginRate = "0.03",
            accountContext = OrderBreakdownAccountContext(
                equity = "60000", otherMaintenanceMargin = "0",
                existingPosition = OrderBreakdownExistingPosition(side = PositionSide.LONG, size = "10", entryPrice = "50000"),
            ),
        )
        val result = Arca.orderBreakdown(opts)
        assertEquals(41515.0, result.estimatedLiquidationPrice!!.toDouble(), 0.5)
    }

    @Test
    fun oppositeSideEqualCloseYieldsNull() {
        val opts = OrderBreakdownOptions(
            amount = "10", amountType = OrderBreakdownAmountType.TOKENS, leverage = 10,
            feeRate = "0.00045", price = "50000", side = OrderSide.SELL, szDecimals = 5,
            maintenanceMarginRate = "0.03",
            accountContext = OrderBreakdownAccountContext(
                equity = "60000", otherMaintenanceMargin = "0",
                existingPosition = OrderBreakdownExistingPosition(side = PositionSide.LONG, size = "10", entryPrice = "50000"),
            ),
        )
        val result = Arca.orderBreakdown(opts)
        assertNull(result.estimatedLiquidationPrice)
    }

    @Test
    fun oppositeSideLargerFlipsSide() {
        val opts = OrderBreakdownOptions(
            amount = "10", amountType = OrderBreakdownAmountType.TOKENS, leverage = 10,
            feeRate = "0.00045", price = "50000", side = OrderSide.SELL, szDecimals = 5,
            maintenanceMarginRate = "0.03",
            accountContext = OrderBreakdownAccountContext(
                equity = "60000", otherMaintenanceMargin = "0",
                existingPosition = OrderBreakdownExistingPosition(side = PositionSide.LONG, size = "4", entryPrice = "50000"),
            ),
        )
        val result = Arca.orderBreakdown(opts)
        assertEquals(58462.5, result.estimatedLiquidationPrice!!.toDouble(), 0.5)
    }

    @Test
    fun omitsLiqWhenMarginAvailNonPositive() {
        val opts = OrderBreakdownOptions(
            amount = "10", amountType = OrderBreakdownAmountType.TOKENS, leverage = 10,
            feeRate = "0.00045", price = "50000", side = OrderSide.BUY, szDecimals = 5,
            maintenanceMarginRate = "0.03",
            accountContext = OrderBreakdownAccountContext(equity = "10000", otherMaintenanceMargin = "0"),
        )
        val result = Arca.orderBreakdown(opts)
        assertNull(result.estimatedLiquidationPrice)
    }

    @Test
    fun omitsLiqWhenMmrNotProvided() {
        val opts = OrderBreakdownOptions(
            amount = "10", amountType = OrderBreakdownAmountType.TOKENS, leverage = 10,
            feeRate = "0.00045", price = "50000", side = OrderSide.BUY, szDecimals = 5,
        )
        val result = Arca.orderBreakdown(opts)
        assertNull(result.estimatedLiquidationPrice)
    }
}
