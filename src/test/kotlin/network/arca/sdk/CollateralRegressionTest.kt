package network.arca.sdk

import kotlinx.serialization.json.Json
import network.arca.sdk.models.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class CollateralRegressionTest {
    private val fixture = """{"account":{"id":"act_margin","realmId":"rlm_margin","name":"margin","createdAt":"2026-09-06","updatedAt":"2026-09-06"},"marginSummary":{"equity":"1000","initialMarginUsed":"600","maintenanceMarginRequired":"0","availableToWithdraw":"1000","totalNtlPos":"12000","totalUnrealizedPnl":"0","totalRawUsd":"1000"},"positions":[{"id":"pos_btc","market":"hl:0:BTC","side":"long","size":"100","entryPrice":"100","leverage":20,"marginUsed":"500","marginMode":"cross","positionValue":"10000","unrealizedPnl":"0"},{"id":"pos_xyz","market":"hl:1:XYZ","side":"long","size":"20","entryPrice":"100","leverage":20,"marginUsed":"100","marginMode":"cross","positionValue":"2000","unrealizedPnl":"0"}],"openOrders":[],"collateralModel":{"crossDexReservationEnforced":true,"crossDexReservationRate":"0.1","totalCollateralUsd":"1000","nativeAvailableUsd":"400","crossDexAvailableUsd":"0"},"stateRefreshIntervalMs":15000,"crossMarginSummary":{"equity":"1000","initialMarginUsed":"600","maintenanceMarginRequired":"0","availableToWithdraw":"1000","totalNtlPos":"12000","totalUnrealizedPnl":"0","totalRawUsd":"1000"}}"""
    private fun book() = Json.decodeFromString<ExchangeState>(fixture)

    @Test fun reversalKeepsCloseWithoutInventingOpeningCapacity() {
        for (side in listOf(PositionSide.LONG, PositionSide.SHORT)) {
            val base = book()
            val state = base.copy(positions = base.positions.map { it.copy(side = side) })
            val data = deriveActiveAssetData(state, "hl:1:XYZ", 100.0, 20, OrderSide.SELL)!!
            assertEquals(20.0, (if (side == PositionSide.LONG) data.maxSellSize else data.maxBuySize).toDouble())
            assertEquals(0.0, (if (side == PositionSide.LONG) data.maxBuySize else data.maxSellSize).toDouble())
        }
    }

    @Test fun revaluationExcludesIsolatedProfitAndLossAndKeepsCapabilities() {
        val base = book()
        val state = base.copy(
            crossMarginSummary = base.marginSummary.copy(equity = "900", initialMarginUsed = "500", totalRawUsd = "900"),
            positions = base.positions.map { if (it.market == "hl:1:XYZ") it.copy(marginMode = MarginMode.ISOLATED, isolatedMargin = "100") else it },
        )
        for (mark in listOf("95", "105")) {
            val marked = state.revalued(mapOf("hl:0:BTC" to "100", "hl:1:XYZ" to mark))
            assertEquals(900.0, marked.crossMarginSummary!!.equity.toDouble())
            assertEquals(base.collateralModel, marked.collateralModel)
            assertEquals(15000L, marked.stateRefreshIntervalMs)
        }
    }
}
