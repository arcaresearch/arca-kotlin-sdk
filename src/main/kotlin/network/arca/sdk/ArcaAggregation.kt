package network.arca.sdk

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.AggregationSource
import network.arca.sdk.models.ChartPointStatus
import network.arca.sdk.models.CreateWatchResponse
import network.arca.sdk.models.EquityHistoryResponse
import network.arca.sdk.models.EquityPoint
import network.arca.sdk.models.ExternalFlowEntry
import network.arca.sdk.models.ObjectValuation
import network.arca.sdk.models.PathAggregation
import network.arca.sdk.models.PnlHistoryResponse
import network.arca.sdk.models.PnlPoint
import network.arca.sdk.models.PnlResponse
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

// `watchEquityChart` / `watchPnlChart` treat a watch as a "live tail" when the
// requested `to` is within this distance of construction time, and slide the
// window forward on every refresh. Watches with `to` further in the past are
// treated as fixed-window watches and stay pinned.
internal const val LIVE_TAIL_THRESHOLD_S: Long = 60

// In the chart streams' boundary timer: if no aggregation event has arrived for
// this much wall-clock time AND the bucket boundary has advanced, treat it as a
// tab freeze / quiet realm and refetch the dense server window. 1.5 x interval
// gives one grace bucket for ordinary network jitter before we suspect the stream.
internal const val BOUNDARY_AGG_SILENCE_FACTOR: Double = 1.5

/** Format an [Instant] to RFC 3339 with fractional seconds (UTC `Z`). */
internal fun iso8601String(instant: Instant): String = DateTimeFormatter.ISO_INSTANT.format(instant)

/**
 * Map a server-side resolution string (the same set returned by `/history`'s
 * `resolution` field) to the seconds-per-bucket the chart streams use to detect
 * bucket-boundary crossings. Unknown strings fall back to 1h.
 */
internal fun chartResolutionSeconds(resolution: String?): Long = when (resolution) {
    "1m" -> 60
    "5m" -> 300
    "15m" -> 900
    "30m" -> 1_800
    "1h", "hour" -> 3_600
    "4h" -> 14_400
    "1d", "day" -> 86_400
    else -> 3_600
}

/**
 * Range presets accepted by [Arca.watchEquityChartLive] / [Arca.watchPnlChartLive]
 * and by [computeChartRange]. Mirror of the TypeScript `ChartRangePreset`.
 */
public enum class ChartRangePreset(public val wire: String) {
    ONE_HOUR("1h"),
    TWENTY_FOUR_HOURS("24h"),
    SEVEN_DAYS("7d"),
    THIRTY_DAYS("30d"),
    THREE_MONTHS("3m"),
    ONE_YEAR("1y"),
    YTD("ytd"),
    ALL("all"),
}

// MARK: - Aggregation, P&L, Equity History

/**
 * Get the valuation for a single Arca object. Uses the same computation path as
 * aggregation (Axiom 10: Observational Consistency).
 *
 * @param path Path of the Arca object.
 */
public suspend fun Arca.getObjectValuation(path: String): ObjectValuation =
    client.get("/objects/valuation", query = mapOf("realmId" to realm, "path" to path))

/**
 * Get aggregated valuation for objects at a path.
 *
 * @param path Object path or path prefix. An exact path (no trailing slash)
 *   returns a single object's valuation; a path prefix (trailing slash) returns
 *   the aggregated valuation for all objects under that prefix.
 * @param asOf Optional timestamp for historical aggregation.
 */
public suspend fun Arca.getPathAggregation(path: String, asOf: String? = null): PathAggregation {
    validatePath(path)
    val query = mutableMapOf("realmId" to realm, "prefix" to path)
    if (asOf != null) query["asOf"] = asOf
    return client.get("/objects/aggregate", query = query)
}

/**
 * Get P&L for objects at a path over a time range.
 *
 * @param path Object path or path prefix.
 * @param from Start timestamp (RFC 3339).
 * @param to End timestamp (RFC 3339).
 */
public suspend fun Arca.getPnl(path: String, from: String, to: String): PnlResponse {
    validatePath(path)
    return client.get(
        "/objects/pnl",
        query = mapOf("realmId" to realm, "prefix" to path, "from" to from, "to" to to),
    )
}

/**
 * Get P&L history (time-series) for objects at a path.
 *
 * @param path Object path or path prefix.
 * @param from Start timestamp (RFC 3339).
 * @param to End timestamp (RFC 3339).
 * @param points Number of samples (default 1000, max 1000). The backend ladder
 *   picks the finest resolution whose bucket count fits within `points`.
 */
public suspend fun Arca.getPnlHistory(
    path: String,
    from: String,
    to: String,
    points: Int = 1000,
): PnlHistoryResponse {
    validatePath(path)
    val key = buildCacheKey(
        "pnlHistory",
        mapOf("target" to path, "kind" to "path", "from" to from, "to" to to, "points" to points.toString()),
    )
    historyCache.get<PnlHistoryResponse>(key)?.let { return it }
    val result: V2PnlHistoryResponse = client.get(
        "/objects/pnl/history",
        query = mapOf(
            "realmId" to realm,
            "target" to path,
            "kind" to "path",
            "from" to from,
            "to" to to,
            "points" to points.toString(),
        ),
    )
    val normalized = PnlHistoryResponse(
        prefix = path,
        from = from,
        to = to,
        points = result.points?.size ?: 0,
        resolution = result.resolution,
        resolutionRequested = result.resolutionRequested,
        serverNow = result.serverNow,
        startingEquityUsd = result.startEquityUsd ?: result.startingEquityUsd ?: "0",
        effectiveFrom = result.effectiveFrom,
        pnlPoints = result.points?.map {
            PnlPoint(
                timestamp = it.ts,
                pnlUsd = it.pnlUsd,
                equityUsd = it.equityUsd,
                status = it.status,
                cumInflowsUsd = it.cumInflowsUsd,
                cumOutflowsUsd = it.cumOutflowsUsd,
                lastEventOpId = it.lastEventOpId,
                midSetId = it.midSetId,
                valueUsd = it.valueUsd,
            )
        } ?: emptyList(),
        externalFlows = result.externalFlows ?: emptyList(),
        midPrices = result.midPrices ?: emptyMap(),
    )
    historyCache.set(key, normalized)
    return normalized
}

/**
 * Get equity history (time-series) for objects at a path.
 *
 * @param path Object path or path prefix.
 * @param from Start timestamp (RFC 3339).
 * @param to End timestamp (RFC 3339).
 * @param points Number of samples (default 1000, max 1000).
 */
public suspend fun Arca.getEquityHistory(
    path: String,
    from: String,
    to: String,
    points: Int = 1000,
): EquityHistoryResponse {
    validatePath(path)
    val key = buildCacheKey(
        "equityHistory",
        mapOf("target" to path, "kind" to "path", "from" to from, "to" to to, "points" to points.toString()),
    )
    historyCache.get<EquityHistoryResponse>(key)?.let { return it }
    val result: V2HistoryResponse = client.get(
        "/objects/aggregate/history",
        query = mapOf(
            "realmId" to realm,
            "target" to path,
            "kind" to "path",
            "from" to from,
            "to" to to,
            "points" to points.toString(),
        ),
    )
    val normalized = EquityHistoryResponse(
        prefix = path,
        from = from,
        to = to,
        points = result.points?.size ?: 0,
        resolution = result.resolution,
        resolutionRequested = result.resolutionRequested,
        serverNow = result.serverNow,
        equityPoints = result.points?.map {
            EquityPoint(
                timestamp = it.ts,
                equityUsd = it.equityUsd,
                status = it.status,
                cumInflowsUsd = it.cumInflowsUsd,
                cumOutflowsUsd = it.cumOutflowsUsd,
                lastEventOpId = it.lastEventOpId,
                midSetId = it.midSetId,
            )
        } ?: emptyList(),
    )
    historyCache.set(key, normalized)
    return normalized
}

/**
 * Create an aggregation watch that tracks a set of sources. When the underlying
 * data changes, `aggregation.updated` events are pushed via WebSocket.
 *
 * @param sources Sources to track.
 */
public suspend fun Arca.createAggregationWatch(
    sources: List<AggregationSource>,
    flowsSince: String? = null,
): CreateWatchResponse {
    val body = arcaJson.encodeToJsonElement(
        CreateWatchRequest.serializer(),
        CreateWatchRequest(realmId = realm, sources = sources, flowsSince = flowsSince),
    )
    return client.post("/aggregations/watch", body = body)
}

/** Get the current aggregation for an existing watch. */
public suspend fun Arca.getWatchAggregation(watchId: String): PathAggregation {
    val response: GetWatchAggregationResponse = client.get("/aggregations/watch/$watchId")
    return response.aggregation
}

/** Destroy an aggregation watch. */
public suspend fun Arca.destroyAggregationWatch(watchId: String) {
    client.delete<JsonElement>("/aggregations/watch/$watchId")
}

/**
 * Compute `from`/`to` ISO timestamps for a chart range preset, anchored to the
 * current wall clock. Mirror of the TypeScript SDK's `computeChartRange`.
 */
public fun Arca.Companion.computeChartRange(range: ChartRangePreset): Pair<String, String> {
    val now = ZonedDateTime.now(ZoneOffset.UTC)
    val from = when (range) {
        ChartRangePreset.ONE_HOUR -> now.minusHours(1)
        ChartRangePreset.TWENTY_FOUR_HOURS -> now.minusHours(24)
        ChartRangePreset.SEVEN_DAYS -> now.minusDays(7)
        ChartRangePreset.THIRTY_DAYS -> now.minusDays(30)
        ChartRangePreset.THREE_MONTHS -> now.minusMonths(3)
        ChartRangePreset.ONE_YEAR -> now.minusYears(1)
        ChartRangePreset.YTD -> now.toLocalDate().withDayOfYear(1).atStartOfDay(ZoneOffset.UTC)
        ChartRangePreset.ALL -> now.minusYears(5)
    }
    return iso8601String(from.toInstant()) to iso8601String(now.toInstant())
}

// MARK: - Request / wire types

@Serializable
private data class CreateWatchRequest(
    val realmId: String,
    val sources: List<AggregationSource>,
    val flowsSince: String? = null,
)

@Serializable
private data class GetWatchAggregationResponse(
    val watchId: String,
    val aggregation: PathAggregation,
)

@Serializable
private data class V2HistoryPoint(
    val ts: String,
    val equityUsd: String,
    val status: ChartPointStatus? = null,
    val cumInflowsUsd: String? = null,
    val cumOutflowsUsd: String? = null,
    val lastEventOpId: String? = null,
    val midSetId: String? = null,
)

@Serializable
private data class V2HistoryResponse(
    val resolution: String? = null,
    val resolutionRequested: String? = null,
    val serverNow: String? = null,
    val points: List<V2HistoryPoint>? = null,
)

@Serializable
private data class V2PnlHistoryPoint(
    val ts: String,
    val pnlUsd: String,
    val equityUsd: String,
    val status: ChartPointStatus? = null,
    val cumInflowsUsd: String? = null,
    val cumOutflowsUsd: String? = null,
    val lastEventOpId: String? = null,
    val midSetId: String? = null,
    val valueUsd: String? = null,
)

@Serializable
private data class V2PnlHistoryResponse(
    val resolution: String? = null,
    val resolutionRequested: String? = null,
    val serverNow: String? = null,
    val startEquityUsd: String? = null,
    val startingEquityUsd: String? = null,
    val effectiveFrom: String? = null,
    val externalFlows: List<ExternalFlowEntry>? = null,
    val midPrices: Map<String, String>? = null,
    val points: List<V2PnlHistoryPoint>? = null,
)
