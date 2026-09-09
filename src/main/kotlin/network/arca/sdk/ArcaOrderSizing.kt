package network.arca.sdk

import java.math.BigDecimal
import java.math.RoundingMode

/** Freeze a reduction using canonical market metadata, never inferred venue precision. */
public suspend fun Arca.normalizedReductionSize(market: String, size: String, fraction: String): String {
    val metadata = market(market) ?: throw IllegalArgumentException("Market metadata unavailable")
    require(metadata.name == market) { "Market identity mismatch" }
    return normalizedReductionSize(size, fraction, metadata.szDecimals)
}

internal fun normalizedReductionSize(size: String, fraction: String, decimals: Int): String {
    val decimal = Regex("^[0-9]+(?:\\.[0-9]+)?$")
    require(decimal.matches(size) && decimal.matches(fraction) && decimals in 0..18) { "Invalid reduction quantity" }
    val quantity = BigDecimal(size)
    val part = BigDecimal(fraction)
    require(quantity.signum() > 0 && part.signum() > 0 && part <= BigDecimal.ONE) { "Invalid reduction quantity" }
    val rounded = quantity.multiply(part).setScale(decimals, RoundingMode.DOWN)
    require(rounded.signum() > 0) { "Reduction below market lot" }
    return rounded.stripTrailingZeros().toPlainString()
}
