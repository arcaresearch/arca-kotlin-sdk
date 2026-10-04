package network.arca.sdk

import network.arca.sdk.models.Candle
import network.arca.sdk.models.CandleInterval
import network.arca.sdk.models.Market
import network.arca.sdk.models.RealmEvent

/** Reversible preference for public display prices and open candles. Financial authority is unchanged. */
public enum class MarketDataPreference { ARCA, HYPERLIQUID }

/** Explicit venue network; never inferred from a realm name or API hostname. */
public enum class HyperliquidNetwork(internal val websocketUrl: String) {
    MAINNET("wss://api.hyperliquid.xyz/ws"), TESTNET("wss://api.hyperliquid-testnet.xyz/ws"),
}

/** Diagnostics for source selection. No source timestamp is invented for Arca frames. */
public data class MarketDataSourceStatus(
    public val preference: MarketDataPreference,
    public val directSubscriptions: Int,
    public val directPriceMarkets: Set<String>,
    public val directCandleMarkets: Set<String>,
    public val lastError: String?,
)

internal data class PublicMarketSubscription(val market: String, val coin: String, val interval: CandleInterval? = null)
internal sealed interface PublicMarketUpdate {
    data class Quote(val market: String, val price: String, val timeMs: Long) : PublicMarketUpdate
    data class Bar(val market: String, val interval: CandleInterval, val candle: Candle) : PublicMarketUpdate
    data class Unavailable(val reason: String) : PublicMarketUpdate
    data class Traffic(val bytes: Int) : PublicMarketUpdate
}

/** Provider boundary: implementations know wire formats/reconnection; the router knows preference. */
internal interface PublicMarketSource {
    fun subscribe(subscriptions: Set<PublicMarketSubscription>)
    fun close()
}

/** Pure routing state, serialized by the owning WebSocketManager. Both transports enter its event bus. */
internal class MarketDataRouter {
    var preference = MarketDataPreference.ARCA
    private var mapping = emptyMap<String, String>()
    private data class Interest(val revision: Long, val markets: Set<String>)
    private val interests = linkedMapOf<String, Interest>()
    val priceInterestMarkets: Set<String> get() = interests.values.flatMap { it.markets }.toSet()
    private val candles = linkedMapOf<Pair<String, CandleInterval>, Int>()
    private val quoteTimes = mutableMapOf<String, Long>()
    private val bars = mutableMapOf<Pair<String, CandleInterval>, Candle>()
    private val closedBars = mutableMapOf<Pair<String, CandleInterval>, Long>()
    var subscriptions: Set<PublicMarketSubscription> = emptySet(); private set
    var lastError: String? = null; private set

    fun configure(preference: MarketDataPreference, markets: Collection<Market>) {
        this.preference = preference
        // venueSymbol is authoritative metadata. Never reconstruct it from a display ticker or dex index.
        val valid = markets.filter { canonicalHL.matches(it.name) && !it.venueSymbol.isNullOrBlank() }
        val unique = valid.groupBy { it.venueSymbol }.filterValues { it.size == 1 }.values.flatten()
        mapping = unique.groupBy { it.name }.filterValues { it.size == 1 }.values.flatten().associate { it.name to it.venueSymbol!! }
        unavailable(null)
        refresh()
    }

    fun register(owner: String) { interests[owner] = Interest(-1, emptySet()) }
    fun update(owner: String, markets: Set<String>, revision: Long) {
        val current = interests[owner] ?: return // late work cannot resurrect a released watch
        if (revision < current.revision) return
        interests[owner] = Interest(revision, markets)
        refresh()
    }
    fun release(owner: String) { interests.remove(owner); refresh() }
    fun acquireCandles(markets: List<String>, intervals: List<CandleInterval>) {
        markets.forEach { m -> intervals.forEach { i -> candles[m to i] = (candles[m to i] ?: 0) + 1 } }
        refresh()
    }
    fun releaseCandles(markets: List<String>, intervals: List<CandleInterval>) {
        markets.forEach { m -> intervals.forEach { i ->
            val key = m to i; val count = (candles[key] ?: 0) - 1
            if (count <= 0) candles.remove(key) else candles[key] = count
        } }
        refresh()
    }
    fun candleRetained(market: String, interval: CandleInterval): Boolean = (candles[market to interval] ?: 0) > 0
    private fun refresh() {
        val wanted = mutableListOf<PublicMarketSubscription>()
        if (preference == MarketDataPreference.HYPERLIQUID) {
            interests.values.flatMap { it.markets }.toSortedSet().forEach { m -> mapping[m]?.let { wanted += PublicMarketSubscription(m, it) } }
            candles.keys.sortedWith(compareBy({ it.first }, { it.second.wire })).forEach { (m, i) ->
                if (i != CandleInterval.FIFTEEN_SECONDS) mapping[m]?.let { wanted += PublicMarketSubscription(m, it, i) }
            }
        }
        // Bound phone receive/decode work and outbound subscription bursts. Overflow stays on Arca.
        subscriptions = wanted.take(64).toSet()
        quoteTimes.keys.retainAll(subscriptions.filter { it.interval == null }.map { it.market }.toSet())
        closedBars.keys.retainAll(candles.keys)
        bars.keys.retainAll(subscriptions.mapNotNull { s -> s.interval?.let { s.market to it } }.toSet())
    }
    fun unavailable(reason: String?) { quoteTimes.clear(); bars.clear(); lastError = reason }
    fun arcaPrices(prices: Map<String, String>): Map<String, String> = prices.filterKeys { it !in quoteTimes }
    fun arcaCandle(event: RealmEvent): Boolean {
        if (event.type == "candle.closed") {
            val interval = CandleInterval.fromWire(event.interval ?: "")
            val key = event.market to interval
            if (event.market != null && interval != null && event.candle != null && key in candles) {
                closedBars[event.market to interval] = maxOf(closedBars[key] ?: 0, event.candle.t)
            }
            return true // authoritative history/corrections always win
        }
        val interval = CandleInterval.entries.firstOrNull { it.wire == event.interval } ?: return true
        val incoming = event.candle ?: return true
        if (incoming.t <= (closedBars[event.market to interval] ?: 0)) return false
        val direct = bars[event.market to interval] ?: return true
        return incoming.t > direct.t // do not replay an older open frame over the selected live bar
    }
    fun direct(update: PublicMarketUpdate, nowMs: Long): RealmEvent? = when (update) {
        is PublicMarketUpdate.Traffic -> null
        is PublicMarketUpdate.Unavailable -> { unavailable(update.reason); null }
        is PublicMarketUpdate.Quote -> {
            if (subscriptions.none { it.market == update.market && it.interval == null } ||
                update.timeMs <= 0 || update.timeMs > nowMs + 30_000 ||
                update.timeMs < (quoteTimes[update.market] ?: 0)) null
            else {
                quoteTimes[update.market] = update.timeMs; lastError = null
                RealmEvent(type = "mids.updated", mids = mapOf(update.market to update.price))
            }
        }
        is PublicMarketUpdate.Bar -> {
            val key = update.market to update.interval
            val c = update.candle
            val previous = bars[key]
            if (subscriptions.none { it.market == update.market && it.interval == update.interval } ||
                c.t <= 0 || c.t % update.interval.milliseconds != 0L || c.t <= (closedBars[key] ?: 0) ||
                c.t > nowMs || c.t <= nowMs - update.interval.milliseconds ||
                previous != null && (c.t < previous.t || c.t == previous.t && c.n < previous.n)) null
            else {
                bars[key] = c; lastError = null
                RealmEvent(type = "candle.updated", market = update.market, interval = update.interval.wire, candle = c)
            }
        }
    }
    fun status(): MarketDataSourceStatus = MarketDataSourceStatus(preference, subscriptions.size, quoteTimes.keys.toSet(), bars.keys.map { it.first }.toSet(), lastError)
    companion object { private val canonicalHL = Regex("hl:[0-9]+:[^:\\s]+") }
}

/**
 * Choose the public market-data preference without replacing watches. Arca is the default and
 * stays subscribed for fallback and finalized candles. Metadata lookup is finite, never polled.
 * Changing back to ARCA takes effect immediately and opens no public venue connection.
 */
public suspend fun Arca.setMarketDataPreference(
    preference: MarketDataPreference,
    network: HyperliquidNetwork = HyperliquidNetwork.MAINNET,
) {
    val epoch = ws.beginMarketDataConfiguration()
    val markets = if (preference == MarketDataPreference.HYPERLIQUID) ensureMetaLoaded().values else emptyList()
    ws.configureMarketData(epoch, preference, network, markets)
}

public val Arca.marketDataSourceStatus: MarketDataSourceStatus get() = ws.marketDataSourceStatus
