package network.arca.sdk

import java.io.File
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * Seam healing for `watchCandleChart`: candles missed while the socket stays
 * nominally connected (e.g. an upstream ingest stall) previously left the
 * chart with a permanent hole — reconnect-driven recovery never fired because
 * there was no reconnect. The fix detects the seam in the live data itself
 * (a live candle skipping past the last held bucket, or a deliverySeq gap)
 * and refetches the affected window so server-side backfill corrections
 * reach already-open charts.
 *
 * The detection function is tested directly; the factory wiring is pinned
 * structurally (same approach as [WatchPnlChartSelfHealingTest]).
 */
class WatchCandleChartSeamHealingTest {

    // MARK: - detectSeamStart (live bucket-gap detection)

    @Test
    fun detectSeamStartFindsSkippedBuckets() {
        // Last held bucket 180000, incoming 420000 skips 240000..360000.
        assertEquals(180_000L, detectSeamStart(prevLatestT = 180_000L, incomingT = 420_000L, intervalMs = 60_000L))
    }

    @Test
    fun detectSeamStartIgnoresAdjacentBucket() {
        assertNull(detectSeamStart(prevLatestT = 180_000L, incomingT = 240_000L, intervalMs = 60_000L))
    }

    @Test
    fun detectSeamStartIgnoresSameBucketUpdate() {
        assertNull(detectSeamStart(prevLatestT = 180_000L, incomingT = 180_000L, intervalMs = 60_000L))
    }

    @Test
    fun detectSeamStartIgnoresOutOfOrderCandle() {
        // A candle.closed arriving after the next bucket's candle.updated.
        assertNull(detectSeamStart(prevLatestT = 240_000L, incomingT = 180_000L, intervalMs = 60_000L))
    }

    @Test
    fun detectSeamStartIgnoresEmptyChart() {
        assertNull(detectSeamStart(prevLatestT = 0L, incomingT = 420_000L, intervalMs = 60_000L))
    }

    // MARK: - structural wiring

    private fun loadCandleChartWatchSource(): String {
        val rel = "src/main/kotlin/network/arca/sdk/ArcaCandleChartWatch.kt"
        var dir: File? = File(System.getProperty("user.dir"))
        while (dir != null) {
            val candidate = File(dir, rel)
            if (candidate.isFile) return candidate.readText()
            dir = dir.parentFile
        }
        throw IllegalStateException("Could not locate $rel from user.dir=${System.getProperty("user.dir")}")
    }

    @Test
    fun watchCandleChartContainsSeamHealingWiring() {
        val source = loadCandleChartWatchSource()

        val markers: List<Pair<String, String>> = listOf(
            "data-driven seam detection in the candle event collector" to "detectSeamStart(",
            "seam heal fetch function" to "suspend fun healSeam",
            "seam heal cooldown constant" to "SEAM_HEAL_COOLDOWN_MS",
            "deliverySeq gap handler registration" to "ws.onGap",
            "gap handler removal on stop" to "removeGapHandler",
            "failed heal re-arms the seam" to "seamFrom = seamFrom?.let { minOf(it, from) } ?: from",
        )
        for ((label, marker) in markers) {
            assertTrue(
                source.contains(marker),
                "watchCandleChart missing marker '$marker' ($label)",
            )
        }
    }
}
