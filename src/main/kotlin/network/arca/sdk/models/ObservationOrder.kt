package network.arca.sdk.models

import java.time.Instant
import java.time.format.DateTimeParseException

/**
 * The instant the platform began the read behind this observation:
 * [ExchangeState.observedAt], or the mirror allocation's `asOf` from platforms
 * that stamp only that. Null when the observation carries no read time.
 * `Instant.parse` keeps every fractional digit of Go's RFC3339Nano.
 */
public val ExchangeState.observationTime: Instant?
    get() = parseRfc3339(observedAt) ?: parseRfc3339(tradingAllocation?.asOf)

/**
 * Whether this observation describes an earlier ledger state than [other].
 * Reads of one account race — a push, a re-read after a gap and an
 * application's own `getExchangeState` can complete in any order — and a
 * frame that resolves later is not therefore newer. Applying an observation
 * only when this is false for the one already applied keeps positions and
 * balances monotonic: a fill never appears, disappears and reappears because
 * a pre-fill read landed late. False when either side carries no read time,
 * so unstamped observations apply as before.
 */
public fun ExchangeState.observedBefore(other: ExchangeState): Boolean {
    val mine = observationTime ?: return false
    val theirs = other.observationTime ?: return false
    return mine.isBefore(theirs)
}

private fun parseRfc3339(raw: String?): Instant? {
    if (raw.isNullOrEmpty()) return null
    return try { Instant.parse(raw) } catch (_: DateTimeParseException) { null }
}
