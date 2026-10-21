package network.arca.sdk

import kotlinx.coroutines.flow.Flow

/** An exact published value with its local transport receipt and SDK publication times.
 * Nanoseconds use System.nanoTime(), not Unix time. Carry this record with the value when
 * measuring UI delay; never match a rendered value against a later diagnostics snapshot.
 * Revision is monotonic within one observer. Quiet values retain their original record.
 * venueTimeMs is present only for Hyperliquid BBO, never inferred for allMids or Arca. */
public data class PriceDeliveryDiagnostic(
    val price: String,
    val revision: Long,
    val source: MarketDataPreference,
    val receivedAtNanos: Long,
    val publishedAtNanos: Long,
    val venueTimeMs: Long?,
)

/** Observe published prices for up to 512 explicit canonical markets, with receipt provenance.
 * This is a diagnostic observer of existing watches, not a network subscription. Collect before
 * starting watchPrices to capture its initial values. Cached values before collection have no
 * provenance and are omitted until they change. Full snapshots, conflated to one; cancel to release.
 * Records describe changed price publication, not latest feed activity or venue-to-screen latency. */
public fun Arca.watchPriceDeliveryDiagnostics(markets: Set<String>): Flow<Map<String, PriceDeliveryDiagnostic>> =
    ws.watchPriceDeliveryDiagnostics(markets)

internal class PriceDeliveryRecorder(val markets: Set<String>) {
    init { require(markets.size <= 512 && markets.all(::isCanonicalMarketId)) { "Use at most 512 explicit canonical markets" } }
    private data class Pending(val price: String, val source: MarketDataPreference, val received: Long, val venue: Long?)
    private val pending = mutableMapOf<String, Pending>()
    private var published = emptyMap<String, PriceDeliveryDiagnostic>()
    private var revision = 0L
    fun stage(prices: Map<String, String>, source: MarketDataPreference, received: Long, venue: Long?) {
        for ((market, price) in prices) if (market in markets) pending[market] = Pending(price, source, received, venue)
    }
    fun clearDirectPending(keep: Set<String> = emptySet()) {
        pending.entries.removeAll { it.value.source == MarketDataPreference.HYPERLIQUID && it.key !in keep }
    }
    fun publish(prices: Map<String, String>, now: Long, committed: Set<String> = prices.keys): Map<String, PriceDeliveryDiagnostic>? {
        val next = published.filter { (market, value) -> prices[market] == value.price }.toMutableMap()
        for ((market, p) in pending) if (market in committed && prices[market] == p.price) {
            next[market] = PriceDeliveryDiagnostic(p.price, ++revision, p.source, p.received, now, p.venue)
        }
        pending.entries.removeAll { it.key in committed && prices[it.key] == it.value.price }
        if (next == published) return null
        published = next.toMap()
        return published
    }
    fun clear() { pending.clear(); published = emptyMap() }
}
