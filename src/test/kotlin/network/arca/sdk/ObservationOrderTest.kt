package network.arca.sdk

import kotlinx.serialization.json.Json
import network.arca.sdk.models.ExchangeState
import network.arca.sdk.models.observationTime
import network.arca.sdk.models.observedBefore
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant

class ObservationOrderTest {
    private val json = Json { ignoreUnknownKeys = true }

    private fun state(observedAt: String? = null, asOf: String? = null): ExchangeState {
        val observed = observedAt?.let { ""","observedAt":"$it"""" } ?: ""
        val allocation = asOf?.let { ""","tradingAllocation":{"asOf":"$it","revision":"rev_1","preferences":{},"projectionUnavailable":true}""" } ?: ""
        return json.decodeFromString(ExchangeState.serializer(), """
            {"account":{"id":"a","realmId":"r","name":"n","createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z"},
             "marginSummary":{"equity":"1","initialMarginUsed":"0","maintenanceMarginRequired":"0","availableToWithdraw":"1","totalNtlPos":"0","totalUnrealizedPnl":"0"},
             "positions":[],"openOrders":[]$observed$allocation}
        """.trimIndent())
    }

    @Test
    fun observedBeforeOrdersByReadTimeAndNeverByArrival() {
        val earlier = state(observedAt = "2026-09-13T06:37:39.706482Z")
        val later = state(observedAt = "2026-09-13T06:37:40.699918Z")
        assertTrue(earlier.observedBefore(later))
        assertFalse(later.observedBefore(earlier))
        assertFalse(later.observedBefore(later), "equal read times are the same observation, not an older one")

        // Go trims trailing zeros; every remaining digit counts.
        assertTrue(state(observedAt = "2026-09-13T06:37:39.706482Z").observedBefore(state(observedAt = "2026-09-13T06:37:39.7065Z")))

        // Platforms that stamp only the allocation's asOf order the same way; the explicit stamp wins.
        assertTrue(state(asOf = "2026-09-13T06:37:39Z").observedBefore(later))
        assertEquals(Instant.parse("2026-09-13T06:37:41Z"), state(observedAt = "2026-09-13T06:37:41Z", asOf = "2026-09-13T06:37:39Z").observationTime)

        // An unstamped observation is never "before" anything: it applies as it always did.
        assertNull(state().observationTime)
        assertFalse(state().observedBefore(later))
        assertFalse(later.observedBefore(state()))
        assertNull(state(observedAt = "not a time").observationTime)
    }
}
