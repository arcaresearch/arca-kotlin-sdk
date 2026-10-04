package network.arca.sdk

import kotlinx.coroutines.flow.StateFlow

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
)

internal class MarketDataDiagnosticsRecorder {
    private val startNs = System.nanoTime()
    private var counters = MarketDataDiagnostics()
    private val serving = mutableMapOf<String, Boolean>()
    private var failedAtMs: Long? = null
    fun retain(interests: Set<String>) { serving.keys.retainAll(interests) }
    fun suspend() { failedAtMs = null }
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
