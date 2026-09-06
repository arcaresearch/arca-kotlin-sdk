package network.arca.sdk

import kotlinx.serialization.json.Json
import network.arca.sdk.models.LeveragePreferenceMode
import network.arca.sdk.models.TradingAllocationState
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class TradingAllocationTest {
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
