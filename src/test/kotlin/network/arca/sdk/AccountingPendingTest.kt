package network.arca.sdk

import java.math.BigDecimal
import kotlinx.serialization.encodeToString
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.AccountingPendingExecution
import network.arca.sdk.models.ExchangeState
import network.arca.sdk.models.PositionSide
import network.arca.sdk.models.ProjectedPositionSource
import network.arca.sdk.models.isAccountingSettled
import network.arca.sdk.models.projectedPositions
import network.arca.sdk.models.revalued
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class AccountingPendingTest {
    private data class Row(val market: String, val side: String, val size: String, val entry: String)

    private fun state(positions: List<Row>, pending: List<AccountingPendingExecution>? = null): ExchangeState {
        val rows = positions.joinToString(",") {
            """{"id":"pos_${it.market}","market":"${it.market}","side":"${it.side}","size":"${it.size}","entryPrice":"${it.entry}","leverage":10,"marginUsed":"500"}"""
        }
        val pendingJson = pending?.let { arcaJson.encodeToString(it) } ?: "null"
        return arcaJson.decodeFromString(ExchangeState.serializer(), """{"account":{"id":"act","realmId":"rlm","name":"a","createdAt":"2026-09-12","updatedAt":"2026-09-12"},
            "marginSummary":{"equity":"10001","initialMarginUsed":"500","maintenanceMarginRequired":"50","availableToWithdraw":"8971","totalNtlPos":"5000","totalUnrealizedPnl":"-0.13","totalRawUsd":"10001"},
            "positions":[$rows],"openOrders":[],"accountingPending":$pendingJson}""")
    }
    private fun close(accounted: String, unaccounted: String) = AccountingPendingExecution("op", "order", "gllt:13", "sell",
        executedSize = "1.148517", accountedSize = accounted, unaccountedSize = unaccounted, executionFinal = true, averagePrice = "4353.4")

    /** The invariant the field exists for: every frame of the accounting window composes to the same book. */
    @Test fun projectedPositionsAreIdenticalAcrossTheAccountingWindow() {
        val frames = listOf(
            state(listOf(Row("gllt:13", "long", "1.148517", "4353.6")), listOf(close("0", "1.148517"))),
            state(listOf(Row("gllt:13", "long", "0.088517", "4353.6")), listOf(close("1.06", "0.088517"))),
            state(listOf(Row("gllt:13", "long", "0.000517", "4353.6")), listOf(close("1.148", "0.000517"))),
            state(emptyList()),
        )
        frames.forEachIndexed { index, frame ->
            assertTrue(frame.projectedPositions().isEmpty(), "frame $index should compose flat")
            assertEquals(index == frames.lastIndex, frame.isAccountingSettled, "frame $index")
        }
    }

    @Test fun reduceIncreaseOpenAndReverse() {
        val reduced = state(listOf(Row("gllt:13", "long", "1.148517", "4353.6")),
            listOf(AccountingPendingExecution("op", null, "gllt:13", "sell", "1.06", "0", "1.06", true, "4300"))).projectedPositions()
        assertEquals(1, reduced.size)
        assertEquals(0, BigDecimal("0.088517").compareTo(reduced[0].size))
        assertEquals(PositionSide.LONG, reduced[0].side)
        assertEquals(0, BigDecimal("4353.6").compareTo(reduced[0].entryPrice), "a reduction keeps its entry")
        assertEquals(ProjectedPositionSource.EXECUTION, reduced[0].source)
        assertEquals("pos_gllt:13", reduced[0].ledger?.id?.value)

        val increased = state(listOf(Row("gllt:13", "long", "2", "100")),
            listOf(AccountingPendingExecution("op", null, "gllt:13", "buy", "1", "0", "1", true, "200"))).projectedPositions()
        assertEquals(0, BigDecimal(3).compareTo(increased[0].size))
        assertEquals(400.0 / 3, increased[0].entryPrice!!.toDouble(), 1e-9)

        val opened = state(listOf(Row("gllt:14", "short", "3", "40")),
            listOf(AccountingPendingExecution("op", null, "gllt:13", "buy", "2", "0", "2", true, "4353.4"))).projectedPositions()
        assertEquals(listOf("gllt:14", "gllt:13"), opened.map { it.market })
        assertEquals(ProjectedPositionSource.LEDGER, opened[0].source)
        assertEquals(PositionSide.LONG, opened[1].side)
        assertEquals(0, BigDecimal(2).compareTo(opened[1].size))
        assertEquals(0, BigDecimal("4353.4").compareTo(opened[1].entryPrice))
        assertNull(opened[1].ledger)

        val reversed = state(listOf(Row("gllt:13", "long", "2", "100")),
            listOf(AccountingPendingExecution("op", null, "gllt:13", "sell", "5", "0", "5", true, "90"))).projectedPositions()
        assertEquals(1, reversed.size)
        assertEquals(PositionSide.SHORT, reversed[0].side)
        assertEquals(0, BigDecimal(3).compareTo(reversed[0].size))
        assertEquals(0, BigDecimal(90).compareTo(reversed[0].entryPrice))
    }

    @Test fun malformedEntriesNeverInventOrEraseAPosition() {
        val broken = state(listOf(Row("gllt:13", "long", "1", "100")), listOf(
            AccountingPendingExecution("op", null, "gllt:13", "sell", "1", "0", "not-a-number", true),
            AccountingPendingExecution("op2", null, "gllt:99", "sideways", "1", "0", "1", true)))
        val rows = broken.projectedPositions()
        assertEquals(1, rows.size)
        assertEquals(ProjectedPositionSource.LEDGER, rows[0].source)
        assertEquals(0, BigDecimal.ONE.compareTo(rows[0].size))
        assertFalse(broken.isAccountingSettled, "malformed entries still mean the observation is not settled")
    }

    @Test fun fieldSurvivesRevaluationAndIsAbsentWhenSettled() {
        val pending = state(listOf(Row("gllt:13", "long", "1", "100")), listOf(close("0", "1.148517")))
        assertEquals(1, pending.revalued(mapOf("gllt:13" to "101")).accountingPending?.size)
        val settled = state(listOf(Row("gllt:13", "long", "1", "100")))
        assertNull(settled.accountingPending)
        assertTrue(settled.isAccountingSettled)
        assertEquals(listOf(ProjectedPositionSource.LEDGER), settled.projectedPositions().map { it.source })
    }
}
