package network.arca.sdk

import kotlinx.serialization.json.Json
import network.arca.sdk.models.ExchangeState
import network.arca.sdk.models.OrderSide
import network.arca.sdk.models.LeveragePreferenceMode
import network.arca.sdk.models.TradingAllocationState
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class TradingAllocationTest {
    @Test
    fun `legacy local sizing cannot bypass mirror allocation`() {
        val state = Json.decodeFromString<ExchangeState>("""{"account":{"id":"a","realmId":"r","name":"n","createdAt":"2026-09-06T00:00:00Z","updatedAt":"2026-09-06T00:00:00Z"},"marginSummary":{"equity":"1000","initialMarginUsed":"100","maintenanceMarginRequired":"50","availableToWithdraw":"900","totalNtlPos":"1000","totalUnrealizedPnl":"0"},"positions":[],"openOrders":[],"tradingAllocation":{"revision":"1","preferences":{},"projectionUnavailable":true}}""")
        assertNull(deriveActiveAssetData(state, "gllt:3", 1000.0, 10, OrderSide.BUY))
    }

    @Test
    fun `unavailable projection preserves exact revision and fixed intent`() {
        val state = Json.decodeFromString<TradingAllocationState>("""{"revision":"9007199254740993","preferences":{"gllt:11":{"mode":"fixed","leverage":8}},"projectionUnavailable":true}""")
        assertEquals("9007199254740993", state.revision)
        assertEquals(LeveragePreferenceMode.FIXED, state.preferences["gllt:11"]?.mode)
        assertEquals(8, state.preferences["gllt:11"]?.leverage)
        assertTrue(state.projectionUnavailable)
        assertNull(state.projection)
    }
}
