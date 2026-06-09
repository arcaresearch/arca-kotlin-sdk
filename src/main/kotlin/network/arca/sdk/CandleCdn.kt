package network.arca.sdk

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.serialization.builtins.ListSerializer
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.Candle
import network.arca.sdk.models.CandleInterval
import okhttp3.OkHttpClient
import okhttp3.Request
import java.time.DayOfWeek
import java.time.Instant
import java.time.ZoneOffset
import java.time.temporal.ChronoField
import java.time.temporal.IsoFields

/**
 * CDN chunk fetching for historical candle data. Mirrors the TypeScript SDK's
 * `candle-cdn.ts` logic: finalized chunks are served from the CDN; open
 * (in-progress) chunks and 404/non-OK chunks fall back to the REST API.
 */
public object CandleCdn {

    internal data class ChunkPeriod(val key: String, val startMs: Long, val endMs: Long)

    private enum class Granularity { DAILY, WEEKLY, MONTHLY }

    private fun granularity(interval: CandleInterval): Granularity = when (interval) {
        CandleInterval.FIFTEEN_SECONDS,
        CandleInterval.ONE_MINUTE,
        CandleInterval.FIVE_MINUTES,
        CandleInterval.FIFTEEN_MINUTES,
        -> Granularity.DAILY
        CandleInterval.ONE_HOUR, CandleInterval.FOUR_HOURS -> Granularity.WEEKLY
        CandleInterval.ONE_DAY -> Granularity.MONTHLY
    }

    private fun dailyChunk(ms: Long): ChunkPeriod {
        val date = Instant.ofEpochMilli(ms).atZone(ZoneOffset.UTC).toLocalDate()
        val start = date.atStartOfDay(ZoneOffset.UTC).toInstant()
        val end = date.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant()
        val key = "%04d-%02d-%02d".format(date.year, date.monthValue, date.dayOfMonth)
        return ChunkPeriod(key, start.toEpochMilli(), end.toEpochMilli())
    }

    private fun weeklyChunk(ms: Long): ChunkPeriod {
        val zdt = Instant.ofEpochMilli(ms).atZone(ZoneOffset.UTC)
        val week = zdt.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)
        val weekYear = zdt.get(IsoFields.WEEK_BASED_YEAR)
        val monday = java.time.LocalDate.of(weekYear, 1, 4)
            .with(IsoFields.WEEK_OF_WEEK_BASED_YEAR, week.toLong())
            .with(ChronoField.DAY_OF_WEEK, 1L)
        val start = monday.atStartOfDay(ZoneOffset.UTC).toInstant()
        val end = monday.plusDays(7).atStartOfDay(ZoneOffset.UTC).toInstant()
        val key = "%04d-W%02d".format(weekYear, week)
        return ChunkPeriod(key, start.toEpochMilli(), end.toEpochMilli())
    }

    private fun monthlyChunk(ms: Long): ChunkPeriod {
        val date = Instant.ofEpochMilli(ms).atZone(ZoneOffset.UTC).toLocalDate().withDayOfMonth(1)
        val start = date.atStartOfDay(ZoneOffset.UTC).toInstant()
        val end = date.plusMonths(1).atStartOfDay(ZoneOffset.UTC).toInstant()
        val key = "%04d-%02d".format(date.year, date.monthValue)
        return ChunkPeriod(key, start.toEpochMilli(), end.toEpochMilli())
    }

    internal fun chunkForTime(interval: CandleInterval, ms: Long): ChunkPeriod = when (granularity(interval)) {
        Granularity.DAILY -> dailyChunk(ms)
        Granularity.WEEKLY -> weeklyChunk(ms)
        Granularity.MONTHLY -> monthlyChunk(ms)
    }

    /** All chunk periods that overlap `[startMs, endMs)`. */
    internal fun chunksForRange(interval: CandleInterval, startMs: Long, endMs: Long): List<ChunkPeriod> {
        if (startMs >= endMs) return emptyList()
        val chunks = mutableListOf<ChunkPeriod>()
        var cursor = startMs
        while (cursor < endMs) {
            val cp = chunkForTime(interval, cursor)
            chunks.add(cp)
            cursor = cp.endMs
        }
        return chunks
    }

    /** The CDN URL for a chunk file. */
    internal fun chunkUrl(baseUrl: String, market: String, interval: CandleInterval, chunkKey: String): String =
        "$baseUrl/candles/$market/${interval.wire}/$chunkKey.json"

    /**
     * Fetch candles from the CDN for a time range, falling back to [apiFallback]
     * for chunks that 404 (not yet published), return a non-OK status, or are
     * still open (current period). Each chunk is fetched independently so one
     * failure does not cancel siblings; cancellation is propagated.
     */
    public suspend fun fetchCandlesFromCdn(
        baseUrl: String,
        market: String,
        interval: CandleInterval,
        startMs: Long,
        endMs: Long,
        httpClient: OkHttpClient,
        logger: ArcaLogger = ArcaLogger.disabled,
        apiFallback: suspend (startMs: Long, endMs: Long) -> List<Candle>,
    ): List<Candle> {
        currentCoroutineContext().ensureActive()
        val nowMs = System.currentTimeMillis()
        val chunks = chunksForRange(interval, startMs, endMs)

        val results: List<List<Candle>> = coroutineScope {
            chunks.map { chunk ->
                async {
                    fetchChunk(baseUrl, market, interval, chunk, startMs, endMs, nowMs, httpClient, logger, apiFallback)
                }
            }.awaitAll()
        }

        currentCoroutineContext().ensureActive()

        val merged = results.flatten().sortedBy { it.t }
        val deduped = mutableListOf<Candle>()
        for (candle in merged) {
            val last = deduped.lastOrNull()
            if (last != null && last.t == candle.t) {
                deduped[deduped.size - 1] = candle
            } else {
                deduped.add(candle)
            }
        }
        return deduped
    }

    private suspend fun fetchChunk(
        baseUrl: String,
        market: String,
        interval: CandleInterval,
        chunk: ChunkPeriod,
        startMs: Long,
        endMs: Long,
        nowMs: Long,
        httpClient: OkHttpClient,
        logger: ArcaLogger,
        apiFallback: suspend (Long, Long) -> List<Candle>,
    ): List<Candle> {
        val meta = mapOf("market" to market, "interval" to interval.wire, "chunkKey" to chunk.key)
        val isClosed = nowMs >= chunk.endMs
        val fallbackStart = maxOf(chunk.startMs, startMs)
        val fallbackEnd = minOf(chunk.endMs - 1, endMs)

        if (!isClosed) {
            return runCatching { apiFallback(fallbackStart, fallbackEnd) }.getOrElse { e ->
                logger.warning("cdn", e, meta) { "open-chunk API fallback failed" }
                emptyList()
            }
        }

        val url = chunkUrl(baseUrl, market, interval, chunk.key)
        return try {
            val request = Request.Builder().url(url).get().build()
            httpClient.newCall(request).await().use { response ->
                when {
                    response.code == 404 -> {
                        logger.debug("cdn", metadata = meta + ("url" to url)) { "chunk 404, using API fallback" }
                        apiFallbackOrEmpty(apiFallback, fallbackStart, fallbackEnd, logger, meta)
                    }
                    response.code !in 200..299 -> {
                        logger.warning("cdn", metadata = meta + mapOf("statusCode" to response.code.toString(), "url" to url)) {
                            "chunk non-OK status, using API fallback"
                        }
                        apiFallbackOrEmpty(apiFallback, fallbackStart, fallbackEnd, logger, meta)
                    }
                    else -> {
                        val body = response.body?.string() ?: "[]"
                        val candles = arcaJson.decodeFromString(ListSerializer(Candle.serializer()), body)
                        candles.filter { it.t in startMs until endMs }
                    }
                }
            }
        } catch (e: Throwable) {
            currentCoroutineContext().ensureActive()
            logger.warning("cdn", e, meta + ("url" to url)) { "chunk fetch failed, using API fallback" }
            apiFallbackOrEmpty(apiFallback, fallbackStart, fallbackEnd, logger, meta)
        }
    }

    private suspend fun apiFallbackOrEmpty(
        apiFallback: suspend (Long, Long) -> List<Candle>,
        startMs: Long,
        endMs: Long,
        logger: ArcaLogger,
        meta: Map<String, String>,
    ): List<Candle> = runCatching { apiFallback(startMs, endMs) }.getOrElse { e ->
        logger.warning("cdn", e, meta) { "API fallback failed" }
        emptyList()
    }
}
