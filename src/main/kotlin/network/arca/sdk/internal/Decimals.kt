package network.arca.sdk.internal

import java.math.BigDecimal

/** Parse a decimal string, or null if it isn't a valid number. */
internal fun parseDecimalOrNull(value: String?): BigDecimal? =
    value?.let { runCatching { BigDecimal(it) }.getOrNull() }

/** Parse a decimal string, or zero if it isn't a valid number. */
internal fun parseDecimalOrZero(value: String?): BigDecimal = parseDecimalOrNull(value) ?: BigDecimal.ZERO

/**
 * Render a [BigDecimal] as a plain (non-scientific) string without trailing
 * zeros, mirroring Swift's `Decimal` string interpolation closely enough for
 * value strings. Zero always renders as `"0"`.
 */
internal fun decToString(value: BigDecimal): String {
    if (value.signum() == 0) return "0"
    return value.stripTrailingZeros().toPlainString()
}
