package network.arca.sdk

import network.arca.sdk.models.PnlChartBaseline
import network.arca.sdk.models.PnlHistoryResponse
import network.arca.sdk.models.PnlPoint
import java.time.Instant
import java.time.OffsetDateTime
import java.util.Locale
import kotlin.math.roundToLong

private const val MIN_PNL_DECIMALS = 2
private const val MAX_PNL_DECIMALS = 9

private fun decimalPlaces(s: String?): Int {
    if (s == null) return 0
    val dot = s.indexOf('.')
    if (dot < 0) return 0
    val exp = s.indexOfFirst { it == 'e' || it == 'E' }.let { if (it < 0) s.length else it }
    return maxOf(0, exp - dot - 1)
}

private fun num(s: String?): Double = s?.toDoubleOrNull() ?: 0.0

private fun parseInstant(s: String): Instant? =
    runCatching { Instant.parse(s) }.getOrNull()
        ?: runCatching { OffsetDateTime.parse(s).toInstant() }.getOrNull()

/**
 * Formats a P&L value with as many decimals as the most precise input it was
 * derived from (at least 2, at most 9), so `equityUsd − pnlUsd` comes out
 * exactly at the inputs' precision instead of wobbling by up to half a cent
 * when P&L alone is rounded.
 */
internal fun formatPnlUsd(value: Double, inputs: List<String?>): String {
    val places = minOf(inputs.fold(MIN_PNL_DECIMALS) { acc, s -> maxOf(acc, decimalPlaces(s)) }, MAX_PNL_DECIMALS)
    val fixed = String.format(Locale.US, "%.${places}f", value)
    return if (fixed.startsWith("-") && fixed.drop(1).all { it == '0' || it == '.' }) fixed.drop(1) else fixed
}

/** Whether two dollar strings round to the same cent. Null only matches null. */
internal fun sameCents(a: String?, b: String?): Boolean {
    if (a == null || b == null) return a == null && b == null
    return (num(a) * 100).roundToLong() == (num(b) * 100).roundToLong()
}

/**
 * The baseline a history response counts P&L from: its first point. Null when
 * the response has no points to count from.
 */
internal fun pnlBaselineFrom(points: List<PnlPoint>, startingEquityUsd: String): PnlChartBaseline? {
    val first = points.firstOrNull() ?: return null
    return PnlChartBaseline(
        timestamp = first.timestamp,
        startingEquityUsd = startingEquityUsd,
        cumInflowsUsd = first.cumInflowsUsd,
        cumOutflowsUsd = first.cumOutflowsUsd,
    )
}

/**
 * Re-derives each point's `pnlUsd` against [baseline] from the point's equity
 * and lifetime cumulative flows. A response for a slid window counts from its
 * own first point; this puts it back on the baseline the chart opened with.
 * Returns [points] untouched when the baseline or any point lacks cumulative
 * flows (older servers).
 */
internal fun rebasePnlPoints(points: List<PnlPoint>, baseline: PnlChartBaseline?): List<PnlPoint> {
    val baseIn = baseline?.cumInflowsUsd ?: return points
    val baseOut = baseline.cumOutflowsUsd ?: return points
    if (!points.all { it.cumInflowsUsd != null && it.cumOutflowsUsd != null }) return points
    val base = num(baseline.startingEquityUsd) - num(baseIn) + num(baseOut)
    return points.map { p ->
        val equity = p.unflooredEquityUsd ?: p.equityUsd
        val adjusted = num(equity) - num(p.cumInflowsUsd) + num(p.cumOutflowsUsd)
        p.copy(
            pnlUsd = formatPnlUsd(
                adjusted - base,
                listOf(equity, p.cumInflowsUsd, p.cumOutflowsUsd, baseline.startingEquityUsd, baseIn, baseOut),
            ),
        )
    }
}

/**
 * The instant a history response's first point describes. A bucket point
 * stamped `t` holds the state through the bucket's close, `t + bucketSeconds`
 * (or the server's clock, for the bucket still open), so a flow committed
 * between the requested `from` and that close is already inside
 * `startingEquityUsd`. Counting live flows from here, rather than from
 * `from`, keeps such a flow from being subtracted twice.
 */
internal fun alignedFlowsSince(history: PnlHistoryResponse, fallback: String): String {
    val first = history.pnlPoints.firstOrNull() ?: return fallback
    val bucketSeconds = history.bucketSeconds?.takeIf { it > 0 } ?: return fallback
    val start = parseInstant(first.timestamp) ?: return fallback
    val cap = history.serverNow?.let(::parseInstant) ?: Instant.now()
    return iso8601String(minOf(start.plusSeconds(bucketSeconds.toLong()), cap))
}
