package network.arca.sdk

/** Opt-in public quote budget. These are payload/receive-work targets, not whole-app or wire limits.
 * Coarse allMids coverage is retained even when its unavoidable baseline exceeds the targets.
 * Priorities use canonical market IDs; higher values win, zero is passive. Candles remain explicit.
 * The policy never changes authoritative account/execution state or the Arca fallback. */
public data class MarketDataBudget(
    public val bytesPerSecond: Double,
    public val messagesPerSecond: Double,
    public val priorities: Map<String, Int> = emptyMap(),
) {
    init { require(bytesPerSecond.isFinite() && bytesPerSecond >= 0 && messagesPerSecond.isFinite() && messagesPerSecond >= 0) }
}

internal data class PublicFeedCost(val bytes: Double, val messages: Double) {
    operator fun plus(other: PublicFeedCost) = PublicFeedCost(bytes + other.bytes, messages + other.messages)
}

/** One bounded sample bucket per active/recent feed. Updates are driven by received frames, no polling. */
internal class PublicFeedCosts {
    private data class Sample(var since: Long, var bytes: Long = 0, var messages: Int = 0)
    private val samples = mutableMapOf<String, Sample>()
    private val estimates = mutableMapOf<String, PublicFeedCost>()
    fun estimate(key: String): PublicFeedCost = estimates[key] ?: if (key.startsWith("@"))
        PublicFeedCost(if (key == "@") 5_000.0 else 650.0, 0.2) else PublicFeedCost(600.0, 4.0)
    fun retain(keys: Set<String>) { samples.keys.retainAll(keys); estimates.keys.retainAll(keys) }
    fun record(key: String, bytes: Int, nowMs: Long): Boolean {
        val sample = samples.getOrPut(key) { Sample(nowMs) }
        sample.bytes += bytes.coerceAtLeast(0); sample.messages++
        val elapsed = nowMs - sample.since
        if (elapsed < 10_000) return false
        val measured = PublicFeedCost(sample.bytes * 1_000.0 / elapsed, sample.messages * 1_000.0 / elapsed)
        val old = estimate(key)
        // Cost increases are prompt; decreases need repeated healthy windows.
        estimates[key] = PublicFeedCost(maxOf(measured.bytes, old.bytes * 0.8 + measured.bytes * 0.2),
            maxOf(measured.messages, old.messages * 0.8 + measured.messages * 0.2))
        samples[key] = Sample(nowMs)
        return true
    }
}

internal fun venueDex(coin: String): String = coin.substringBefore(':', "")

/** Rank within two independent budgets; charge measured cost, not a uniform asset count.
 * Re-evaluated only on demand/budget changes or ten-second cost windows. Existing selections
 * win ties, which prevents equal-priority rows from churning as measured costs move. */
internal fun budgetSubscriptions(mapping: Map<String, String>, budget: MarketDataBudget,
    costs: PublicFeedCosts, previous: Set<PublicMarketSubscription>, slots: Int): Set<PublicMarketSubscription> {
    val groups = mapping.keys.groupBy { venueDex(mapping.getValue(it)) }
    val selected = mutableSetOf<String>()
    val previousBbo = previous.filter { it.interval == null && it.dex == null }.map { it.market }.toSet()
    fun total(plan: Set<PublicMarketSubscription>): PublicFeedCost = plan.fold(PublicFeedCost(0.0, 0.0)) { sum, sub ->
        sum + costs.estimate(sub.dex?.let { "@$it" } ?: sub.market)
    }
    fun fits(plan: Set<PublicMarketSubscription>): Boolean = total(plan).let {
        plan.size <= slots && it.bytes <= budget.bytesPerSecond && it.messages <= budget.messagesPerSecond
    }
    fun plan(): Set<PublicMarketSubscription> {
        var result = buildSet {
            groups.toSortedMap().forEach { (dex, markets) ->
                if (markets.any { it !in selected }) add(PublicMarketSubscription("", "", dex = dex))
                markets.filter { it in selected }.forEach { add(PublicMarketSubscription(it, mapping.getValue(it))) }
            }
        }
        groups.toSortedMap().forEach { (dex, markets) ->
            val remainingCost = markets.filter { it !in selected }.fold(PublicFeedCost(0.0, 0.0)) { sum, m -> sum + costs.estimate(m) }
            if (remainingCost.bytes <= costs.estimate("@$dex").bytes) {
                val candidate = result.filterNot { it.dex == dex }.toSet() + markets.map { PublicMarketSubscription(it, mapping.getValue(it)) }
                if (fits(candidate)) result = candidate
            }
        }
        return result
    }
    val ranked = mapping.keys.sortedWith(
        compareByDescending<String> { budget.priorities[it] ?: 0 }.thenByDescending { it in previousBbo }.thenBy { it })
    var result = plan()
    for (market in ranked) {
        selected += market
        val candidate = plan(); val cost = total(candidate)
        if (candidate.size <= slots && cost.bytes <= budget.bytesPerSecond && cost.messages <= budget.messagesPerSecond) result = candidate
        else selected -= market
    }
    // Coarse coverage has first claim on slots; >64 DEXes falls back to Arca.
    return result.sortedWith(compareBy<PublicMarketSubscription> { it.dex == null }.thenBy { it.dex ?: it.market }).take(slots).toSet()
}

/** Install or remove the optional public quote policy without replacing stream owners. */
public fun Arca.setMarketDataBudget(budget: MarketDataBudget?) { ws.setMarketDataBudget(budget) }
