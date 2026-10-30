package network.arca.sdk

import network.arca.sdk.models.PnlChartBaseline
import network.arca.sdk.models.PnlHistoryResponse
import network.arca.sdk.models.PnlPoint
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File
import java.time.Instant

/**
 * Gobi letter 2026-10-30 ("P&L chart baseline"): once a live `watchPnlChart`
 * window slid, refreshed history counted P&L from the new window start while
 * the live point kept the opening start, so one update carried two baselines.
 */
class PnlBaselineTest {

    private fun point(ts: String, pnl: String, equity: String, cumIn: String? = null, cumOut: String? = null, unfloored: String? = null) =
        PnlPoint(timestamp = ts, pnlUsd = pnl, equityUsd = equity, unflooredEquityUsd = unfloored, cumInflowsUsd = cumIn, cumOutflowsUsd = cumOut)

    private val baseline = PnlChartBaseline(
        timestamp = "2026-01-01T00:00:00Z",
        startingEquityUsd = "5000.00",
        cumInflowsUsd = "1000.00",
        cumOutflowsUsd = "0.00",
    )

    @Test
    fun slidWindowIsRebasedOntoOpeningBaseline() {
        // The window slid to 00:01 and the server counted from there.
        val slid = listOf(
            point("2026-01-01T00:01:00Z", "0.00", "5100.00", "1000.00", "0.00"),
            point("2026-01-01T00:02:00Z", "50.00", "6150.00", "2000.00", "0.00"),
            point("2026-01-01T00:03:00Z", "-60.00", "4040.00", "2000.00", "2000.00"),
        )
        assertEquals(listOf("100.00", "150.00", "40.00"), rebasePnlPoints(slid, baseline).map { it.pnlUsd })
    }

    @Test
    fun rebaseUsesUnflooredEquity() {
        val clamped = listOf(point("2026-01-01T00:01:00Z", "0.00", "0.00", "1000.00", "0.00", unfloored = "-50.00"))
        assertEquals("-5050.00", rebasePnlPoints(clamped, baseline).single().pnlUsd)
    }

    @Test
    fun rebaseLeavesPointsWithoutFlowTotalsUntouched() {
        val withFlows = listOf(point("2026-01-01T00:01:00Z", "0.00", "5100.00", "1000.00", "0.00"))
        val mixed = withFlows + point("2026-01-01T00:02:00Z", "10.00", "5110.00")
        assertSame(withFlows, rebasePnlPoints(withFlows, PnlChartBaseline(baseline.timestamp, "5000.00")))
        assertSame(mixed, rebasePnlPoints(mixed, baseline))
        assertSame(withFlows, rebasePnlPoints(withFlows, null))
    }

    @Test
    fun baselineIsTheFirstPoint() {
        val pts = listOf(point("2026-01-01T00:00:00Z", "0.00", "5000.00", "1000.00", "0.00"))
        assertEquals(baseline, pnlBaselineFrom(pts, "5000.00"))
        assertNull(pnlBaselineFrom(emptyList(), "0"))
    }

    @Test
    fun livePnlKeepsInputPrecision() {
        // Formatting to cents made equityUsd − pnlUsd wobble by up to half a cent.
        assertEquals("0.1234", formatPnlUsd(0.1234, listOf("5000.1234", "5000.00")))
        assertEquals("500.00", formatPnlUsd(500.0, listOf("5500", "5000")))
        assertEquals("0.333333333", formatPnlUsd(1.0 / 3.0, listOf("0.333333333333333")))
        assertEquals("0.00", formatPnlUsd(-0.000001, listOf("1.00")))
    }

    @Test
    fun sameCentsComparesAtCentPrecision() {
        assertTrue(sameCents("0.1201", "0.1249"))
        assertFalse(sameCents("0.12", "0.13"))
        assertTrue(sameCents(null, null))
        assertFalse(sameCents("0.12", null))
    }

    private fun history(points: List<PnlPoint>, bucketSeconds: Int?, serverNow: String?) = PnlHistoryResponse(
        prefix = "/w/", from = "2026-01-01T10:07:00Z", to = "2026-01-01T12:00:00Z", points = points.size,
        bucketSeconds = bucketSeconds, serverNow = serverNow, startingEquityUsd = "0", pnlPoints = points,
    )

    @Test
    fun flowsSinceIsTheFirstBucketClose() {
        val h = history(listOf(point("2026-01-01T10:00:00Z", "0", "0")), 3600, "2026-01-01T12:00:00Z")
        assertEquals(Instant.parse("2026-01-01T11:00:00Z"), Instant.parse(alignedFlowsSince(h, "x")))
    }

    @Test
    fun flowsSinceStopsAtServerClockForOpenBucket() {
        val h = history(listOf(point("2026-01-01T10:00:00Z", "0", "0")), 3600, "2026-01-01T10:30:00Z")
        assertEquals(Instant.parse("2026-01-01T10:30:00Z"), Instant.parse(alignedFlowsSince(h, "x")))
    }

    @Test
    fun flowsSinceFallsBackWithoutPointsOrBucketSize() {
        assertEquals("from", alignedFlowsSince(history(emptyList(), 60, null), "from"))
        assertEquals("from", alignedFlowsSince(history(listOf(point("2026-01-01T10:00:00Z", "0", "0")), null, null), "from"))
    }

    /**
     * `watchPnlChart` has no HTTP + WebSocket fixture (see
     * WatchPnlChartSelfHealingTest), so pin the wiring: every refetch path goes
     * through the rebasing installer, and live P&L is never rounded to cents.
     */
    @Test
    fun watchPnlChartRebasesEveryRefetchAndKeepsLivePrecision() {
        val rel = "src/main/kotlin/network/arca/sdk/ArcaChartWatch.kt"
        var dir: File? = File(System.getProperty("user.dir"))
        var source: String? = null
        while (dir != null && source == null) {
            File(dir, rel).takeIf { it.isFile }?.let { source = it.readText() }
            dir = dir.parentFile
        }
        val text = source ?: error("Could not locate $rel")
        val start = text.indexOf("fun Arca.watchPnlChart(")
        val end = text.indexOf("fun Arca.watchEquityChartLive(")
        assertTrue(start in 0 until end, "watchPnlChart not found")
        val body = text.substring(start, end)

        assertTrue(body.contains("alignedFlowsSince(history"), "flowsSince must be the first bucket's close")
        assertTrue(body.contains("rebasePnlPoints(history.pnlPoints"), "opening history must be on the stream baseline")
        assertEquals(2, body.split("absorbHistoryLocked(fresh)").size - 1, "both the gap handler and refreshHistory must install history through absorbHistoryLocked")
        assertFalse(body.contains("fresh.pnlPoints.filter"), "a refetch must not install server points without rebasing")
        assertFalse(body.contains("%.2f"), "live P&L must keep the precision of its inputs")
        assertTrue(body.contains("baseline = baseline)"), "updates must carry the baseline")
    }
}
