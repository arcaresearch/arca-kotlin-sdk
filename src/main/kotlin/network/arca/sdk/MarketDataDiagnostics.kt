package network.arca.sdk

import kotlinx.coroutines.flow.StateFlow

/** Latest direct connection attempt, using System.nanoTime(). Null means unobserved.
 * Compare retry wait, handshake, first message, and first accepted price separately.
 * This does not measure OS network availability; correlate with app connectivity callbacks. */
public data class MarketConnectionTiming(
    val attemptAtNanos: Long? = null,
    val openedAtNanos: Long? = null,
    val firstMessageAtNanos: Long? = null,
    val firstPriceAtNanos: Long? = null,
    val lastRetryScheduledAtNanos: Long? = null,
    val lastRetryDelayMs: Long? = null,
)

/** Cumulative counters for one SDK client lifetime. Payload bytes exclude TLS/framing;
 * Arca bytes cover its shared socket. Routed interested quote counts are not venue latency,
 * price movement or GPU presentation. No per-market identifiers. */
public data class MarketDataDiagnostics(
    val elapsedMs: Long = 0,
    val preference: MarketDataPreference = MarketDataPreference.ARCA,
    val requestedPriceMarkets: Int = 0,
    val directSubscriptions: Int = 0,
    val arcaServingMarkets: Int = 0,
    val hyperliquidServingMarkets: Int = 0,
    val arcaConnected: Boolean = false,
    val arcaDisconnects: Long = 0,
    val hyperliquidRecoveries: Long = 0,
    val hyperliquidRecoveryMs: Long = 0,
    val firstPriceMs: Long? = null,
    val arcaPriceValues: Long = 0,
    val hyperliquidPriceValues: Long = 0,
    val arcaCandleFrames: Long = 0,
    val hyperliquidCandleFrames: Long = 0,
    val hyperliquidFailures: Long = 0,
    val arcaPayloadBytes: Long = 0,
    val hyperliquidPayloadBytes: Long = 0,
    val hyperliquidConnection: MarketConnectionTiming = MarketConnectionTiming(),
)

internal class MarketDataDiagnosticsRecorder {
    private val startNs = System.nanoTime()
    private var counters = MarketDataDiagnostics()
    private val serving = mutableMapOf<String, Boolean>()
    private var failedAtMs: Long? = null
    fun retain(interests: Set<String>) { serving.keys.retainAll(interests) }
    fun suspend() { failedAtMs = null; counters = counters.copy(hyperliquidConnection = MarketConnectionTiming()) }
    fun connection(update: PublicMarketUpdate.Connection) {
        val old = counters.hyperliquidConnection
        val start = update.attemptAtNanos ?: return
        if (old.attemptAtNanos != null && start < old.attemptAtNanos) return
        if (old.attemptAtNanos == start && (update.openedAtNanos == null || update.openedAtNanos == old.openedAtNanos) && update.retryAtNanos == null) return
        val next = if (old.attemptAtNanos == start) old else MarketConnectionTiming(attemptAtNanos = start,
            firstMessageAtNanos = old.firstMessageAtNanos?.takeIf { it >= start }, firstPriceAtNanos = old.firstPriceAtNanos?.takeIf { it >= start },
            lastRetryScheduledAtNanos = old.lastRetryScheduledAtNanos, lastRetryDelayMs = old.lastRetryDelayMs)
        val newerRetry = update.retryAtNanos != null &&
            (next.lastRetryScheduledAtNanos == null || update.retryAtNanos >= next.lastRetryScheduledAtNanos)
        counters = counters.copy(hyperliquidConnection = next.copy(
            openedAtNanos = update.openedAtNanos ?: next.openedAtNanos,
            lastRetryScheduledAtNanos = if (newerRetry) update.retryAtNanos else next.lastRetryScheduledAtNanos,
            lastRetryDelayMs = if (newerRetry) update.retryDelayMs else next.lastRetryDelayMs))
    }
    fun response(at: Long): Boolean {
        val t = counters.hyperliquidConnection
        if (t.firstMessageAtNanos != null) return false
        counters = counters.copy(hyperliquidConnection = t.copy(firstMessageAtNanos = at))
        return true
    }
    fun acceptedPrice(at: Long): Boolean {
        val t = counters.hyperliquidConnection
        if (t.firstPriceAtNanos != null) return false
        counters = counters.copy(hyperliquidConnection = t.copy(firstPriceAtNanos = at))
        return true
    }
    fun connected(connected: Boolean) {
        counters = counters.copy(arcaConnected = connected, arcaDisconnects = counters.arcaDisconnects + if (counters.arcaConnected && !connected) 1 else 0)
    }
    fun recovered() {
        val failed = failedAtMs ?: return
        counters = counters.copy(hyperliquidRecoveries = counters.hyperliquidRecoveries + 1, hyperliquidRecoveryMs = counters.hyperliquidRecoveryMs + (elapsedMs - failed).coerceAtLeast(0))
        failedAtMs = null
    }
    fun prices(prices: Map<String, String>, direct: Boolean, interests: Set<String>) {
        var count = 0L
        prices.keys.forEach { if (it in interests) { serving[it] = direct; ++count } }
        if (count == 0L) return
        counters = if (direct) counters.copy(firstPriceMs = counters.firstPriceMs ?: elapsedMs, hyperliquidPriceValues = counters.hyperliquidPriceValues + count)
            else counters.copy(firstPriceMs = counters.firstPriceMs ?: elapsedMs, arcaPriceValues = counters.arcaPriceValues + count)
    }
    fun candle(direct: Boolean) { counters = if (direct) counters.copy(hyperliquidCandleFrames = counters.hyperliquidCandleFrames + 1) else counters.copy(arcaCandleFrames = counters.arcaCandleFrames + 1) }
    fun bytes(bytes: Int, direct: Boolean) { counters = if (direct) counters.copy(hyperliquidPayloadBytes = counters.hyperliquidPayloadBytes + bytes.coerceAtLeast(0)) else counters.copy(arcaPayloadBytes = counters.arcaPayloadBytes + bytes.coerceAtLeast(0)) }
    fun failure() { counters = counters.copy(hyperliquidFailures = counters.hyperliquidFailures + 1); if (failedAtMs == null) failedAtMs = elapsedMs }
    private val elapsedMs get() = (System.nanoTime() - startNs) / 1_000_000
    fun snapshot(router: MarketDataRouter): MarketDataDiagnostics {
        val interests = router.priceInterestMarkets
        serving.keys.retainAll(interests)
        val direct = serving.values.count { it }
        return counters.copy(elapsedMs = elapsedMs, preference = router.preference,
            requestedPriceMarkets = interests.size, directSubscriptions = router.subscriptions.size,
            arcaServingMarkets = serving.size - direct, hyperliquidServingMarkets = direct)
    }
}

/** Current cumulative counters. Reading never requests network data. */
public val Arca.marketDataDiagnostics: MarketDataDiagnostics get() = ws.marketDataDiagnostics
/** Conflated, event-driven diagnostics: at most 1 Hz during traffic plus source/interest changes.
 * No polling, extra socket or market subscription. Cancel collection to release the observer. */
public fun Arca.watchMarketDataDiagnostics(): StateFlow<MarketDataDiagnostics> = ws.watchMarketDataDiagnostics()
