package network.arca.sdk

import java.io.File
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test

/**
 * Bug regression: prior to this fix, `watchPnlChart` was missing the
 * self-healing wiring (`resumeStream`, `authenticatedStream`, boundary timer,
 * multi-bucket gap detection, live-tail sliding window) that `watchEquityChart`
 * had already received. End-to-end behaviour testing for the full chart factory
 * wiring requires a mocked HTTP + WebSocket fixture pair; this file instead pins
 * the **structural parity** between the equity and P&L chart factories by
 * asserting both function bodies contain the same set of self-healing markers.
 * If you remove a marker from one factory, you must remove it from the other —
 * or both.
 *
 * This is the Kotlin port of Swift's `WatchPnlChartSelfHealingTests`. The marker
 * strings are the Kotlin equivalents (e.g. `slideIfLiveLocked` for Swift's
 * `slideIfLive`, `jobs.forEach { it.cancel() }` for the per-task `.cancel()`
 * calls).
 */
class WatchPnlChartSelfHealingTest {

    private fun loadChartWatchSource(): String {
        // Gradle runs tests with user.dir == the module root (sdk/kotlin), but
        // walk upward defensively in case the working directory differs.
        val rel = "src/main/kotlin/network/arca/sdk/ArcaChartWatch.kt"
        var dir: File? = File(System.getProperty("user.dir"))
        while (dir != null) {
            val candidate = File(dir, rel)
            if (candidate.isFile) return candidate.readText()
            dir = dir.parentFile
        }
        throw IllegalStateException("Could not locate $rel from user.dir=${System.getProperty("user.dir")}")
    }

    /**
     * Returns the substring containing the body of the named function, from its
     * `fun Arca.<name>(` declaration up to the next top-level marker.
     */
    private fun functionBody(source: String, named: String, until: String): String? {
        val start = source.indexOf("fun Arca.$named(")
        if (start < 0) return null
        val after = source.substring(start)
        val end = after.indexOf(until)
        return if (end < 0) after else after.substring(0, end)
    }

    @Test
    fun equityAndPnlChartFactoriesShareSelfHealingMarkers() {
        val source = loadChartWatchSource()

        val equityBody = functionBody(source, named = "watchEquityChart", until = "fun Arca.watchPnlChart(")
            ?: fail("watchEquityChart not found in source")
        val pnlBody = functionBody(source, named = "watchPnlChart", until = "fun Arca.watchEquityChartLive(")
            ?: fail("watchPnlChart not found in source")

        val markers: List<Pair<String, String>> = listOf(
            "resume task subscribes to ws.resumeStream" to "ws.resumeStream",
            "auth task subscribes to ws.authenticatedStream" to "ws.authenticatedStream",
            "boundary timer guards on agg-silence factor" to "BOUNDARY_AGG_SILENCE_FACTOR",
            "multi-bucket gap detection refetches dense window" to "Multi-bucket gap",
            "live-tail sliding window helper" to "slideIfLiveLocked",
            "live-tail threshold check vs LIVE_TAIL_THRESHOLD_S" to "LIVE_TAIL_THRESHOLD_S",
            "previous-live capture for boundary point" to "previousLiveEquity",
            "sliding window state (cache key)" to "windowFrom",
            "background jobs cancelled on cleanup" to "jobs.forEach { it.cancel() }",
        )

        for ((label, marker) in markers) {
            assertTrue(
                equityBody.contains(marker),
                "watchEquityChart missing marker '$marker' ($label)",
            )
            assertTrue(
                pnlBody.contains(marker),
                "watchPnlChart missing marker '$marker' ($label) — Bug regression: the P&L chart must mirror the equity chart's self-healing wiring.",
            )
        }
    }

    /**
     * The previous-bucket boundary point in the agg-tick path used to copy the
     * LAST HISTORICAL point's pnl/equity, which is the *previously closed*
     * bucket — wrong by one bucket. The fix captures the live values that were
     * current right before the boundary (mirroring TS `PnlChartStream`'s
     * `prevPnlUsd`/`prevEquityUsd` capture). This test ensures we don't silently
     * regress to the old pattern.
     */
    @Test
    fun watchPnlChartCapturesPreviousLiveValuesForBoundaryPoint() {
        val source = loadChartWatchSource()
        val pnlBody = functionBody(source, named = "watchPnlChart", until = "fun Arca.watchEquityChartLive(")
            ?: fail("watchPnlChart not found in source")

        assertTrue(
            pnlBody.contains("previousLiveEquity") && pnlBody.contains("previousLivePnl"),
            "watchPnlChart must capture previousLiveEquity / previousLivePnl BEFORE absorbing the new agg, so a boundary cross emits the values that were current at the boundary.",
        )

        // The boundary point must be built from the captured `previousLivePnl`,
        // not from the last historical point's pnl/equity.
        assertTrue(
            pnlBody.contains("pnlUsd = formatUsd(previousLivePnl)"),
            "watchPnlChart's boundary point must use the captured previousLivePnl — not a historical-tail copy — to avoid an off-by-one boundary value.",
        )

        // The capture must happen BEFORE the new agg is absorbed into liveEquity.
        val captureIdx = pnlBody.indexOf("previousLiveEquityStr = liveEquity")
        val absorbIdx = pnlBody.indexOf("liveEquity = agg.totalEquityUsd")
        assertTrue(
            captureIdx in 0 until absorbIdx,
            "watchPnlChart must capture the previous live equity BEFORE overwriting liveEquity with the new agg.",
        )
    }

    /**
     * Gobi v1.0.0 feedback (2026-06-09): the SDK-appended live point derives
     * from the live aggregation (equity including unrealized P&L), while
     * historical buckets come from the server projection — when the two bases
     * diverge, the chart draws a cliff at the tip that clients could only
     * detect by timestamp inference. Every synthetic live point ("now"-stamped
     * construction) must therefore carry `status = ChartPointStatus.OPEN` so
     * consumers can identify and handle it explicitly.
     *
     * Boundary-promotion points (stamped `Instant.ofEpochSecond(...)`) are
     * intentionally NOT stamped — they become historical.
     */
    @Test
    fun everySyntheticLivePointIsStampedOpen() {
        val source = loadChartWatchSource()
        val equityBody = functionBody(source, named = "watchEquityChart", until = "fun Arca.watchPnlChart(")
            ?: fail("watchEquityChart not found in source")
        val pnlBody = functionBody(source, named = "watchPnlChart", until = "fun Arca.watchEquityChartLive(")
            ?: fail("watchPnlChart not found in source")

        for ((name, body) in listOf("watchEquityChart" to equityBody, "watchPnlChart" to pnlBody)) {
            val liveStamp = "timestamp = iso8601String(Instant.now())"
            var idx = body.indexOf(liveStamp)
            var count = 0
            while (idx >= 0) {
                count++
                // The OPEN stamp must appear within the same construction —
                // a short window after the timestamp argument.
                val window = body.substring(idx, minOf(body.length, idx + 220))
                assertTrue(
                    window.contains("status = ChartPointStatus.OPEN"),
                    "$name: synthetic live point #$count is missing `status = ChartPointStatus.OPEN` — " +
                        "consumers must be able to identify the SDK-appended live tip without timestamp inference. Window:\n$window",
                )
                idx = body.indexOf(liveStamp, idx + 1)
            }
            assertTrue(count >= 3, "$name: expected at least 3 synthetic live point constructions, found $count")
        }
    }
}
