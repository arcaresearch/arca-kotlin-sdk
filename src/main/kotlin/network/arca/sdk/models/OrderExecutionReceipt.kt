package network.arca.sdk.models

import java.math.BigDecimal
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import network.arca.sdk.internal.arcaJson

/** Execution proof; not a complete order or journal history. */
@Serializable
public data class OrderExecutionReceipt(
    public val orderId: String,
    public val status: String,
    public val filledSize: String,
    public val requestedSize: String? = null,
    public val remainingSize: String? = null,
    public val executionState: String,
    public val fulfillmentState: String,
    public val remainingDisposition: String,
    public val avgFillPrice: String? = null,
    public val averagePriceFinal: Boolean = false,
    public val averagePriceSource: String = "venue_aggregate",
    public val fillsComplete: Boolean = false,
    public val objectId: String,
    public val operationId: String,
) {
    /**
     * Refine with this account's merged fill watch. Only recorded rows with
     * matching original order-operation and venue-order identities contribute.
     * Stable execution IDs deduplicate replay. Incomplete/conflicting evidence
     * preserves this receipt. Recorded VWAP uses 18 fractional digits, half-even;
     * original execution/quantity fields never change and final never downgrades.
     */
    public fun refined(fills: List<Fill>): OrderExecutionReceipt {
        fun exact(value: String?): BigDecimal? = decimal(value)?.takeIf {
            value!!.filter { c -> c != '.' }.trimStart('0').length <= 38
        }
        fun representable(value: BigDecimal) = value.stripTrailingZeros().precision() <= 38
        if (averagePriceFinal || operationId.isEmpty() || orderId.isEmpty()) return this
        if (status.uppercase() !in setOf("FILLED", "CANCELLED", "CANCELED", "EXPIRED", "FAILED", "REJECTED")) return this
        val executed = exact(filledSize)?.takeIf { it.signum() > 0 } ?: return this
        data class Execution(val size: BigDecimal, val price: BigDecimal, val market: String, val side: OrderSide?)
        val unique = LinkedHashMap<String, Execution>()
        var market: String? = null
        for (fill in fills) {
            if (fill.orderId != orderId || fill.operationId.isNullOrEmpty()) continue
            if (fill.orderOperationId != operationId || fill.market.isEmpty()) return this
            val size = exact(fill.size)?.takeIf { it.signum() > 0 } ?: return this
            val price = exact(fill.price)?.takeIf { it.signum() > 0 } ?: return this
            if (market != null && market != fill.market) return this
            market = fill.market
            val id = fill.fillId?.takeIf { it.isNotEmpty() } ?: fill.id.takeIf { it.isNotEmpty() } ?: return this
            val previous = unique[id]
            if (previous != null && (previous.size.compareTo(size) != 0 || previous.price.compareTo(price) != 0 || previous.market != fill.market || previous.side != fill.side)) return this
            unique[id] = Execution(size, price, fill.market, fill.side)
        }
        if (unique.isEmpty()) return this
        var quantity = BigDecimal.ZERO
        var notional = BigDecimal.ZERO
        for (execution in unique.values) {
            val product = execution.size * execution.price
            quantity += execution.size
            notional += product
            if (!representable(product) || !representable(quantity) || !representable(notional)) return this
        }
        if (quantity.compareTo(executed) != 0) return this
        val average = notional.divide(quantity, 18, java.math.RoundingMode.HALF_EVEN).stripTrailingZeros()
        if (average.signum() <= 0 || !representable(average * quantity)) return this
        return copy(avgFillPrice = average.toPlainString(), averagePriceFinal = true,
            averagePriceSource = "ledger_vwap", fillsComplete = true)
    }

    internal companion object {
        fun decimal(value: String?): BigDecimal? = value?.takeIf { it.matches(Regex("^[0-9]+(?:\\.[0-9]+)?$")) }?.toBigDecimalOrNull()
        fun from(operation: Operation, objectId: String, update: OrderExecutionUpdate.Value? = null, originalInput: String? = null): OrderExecutionReceipt? {
            if (operation.state != OperationState.COMPLETED && update == null) return null
            val observedInput = runCatching { operation.input?.let { arcaJson.parseToJsonElement(it).jsonObject } }.getOrNull()
            val observedAccount = observedInput?.get("exchangeObjectId")?.jsonPrimitive?.contentOrNull
            if (observedAccount != null && observedAccount != objectId) return null
            val input = if (originalInput == null) observedInput else runCatching { arcaJson.parseToJsonElement(originalInput).jsonObject }.getOrNull()
            val account = input?.get("exchangeObjectId")?.jsonPrimitive?.contentOrNull
            if (account != null && account != objectId) return null
            val value = update ?: runCatching { operation.outcome?.let { arcaJson.decodeFromString<OrderExecutionUpdate.Value>(it) } }.getOrNull() ?: return null
            val orderId = (value.orderId ?: value.id)?.takeIf { it.isNotEmpty() } ?: return null
            val quantity = decimal(value.filledSize) ?: return null
            val status = when (val rawStatus = value.status.uppercase()) {
                "REJECTED" -> "FAILED"
                "EXPIRED", "CANCELED" -> "CANCELLED"
                else -> rawStatus
            }
            if (status !in setOf("FILLED", "CANCELLED", "FAILED", "REJECTED")) return null
            val size = input?.get("size")?.jsonPrimitive?.contentOrNull
            val requested = decimal(size)
            val partial = requested != null && quantity < requested
            val effect = runCatching { input?.get("gllPrepared")?.jsonObject?.get("request")?.jsonObject?.get("Effect")?.jsonPrimitive?.intOrNull }.getOrNull()
            val ioc = input?.get("timeInForce")?.jsonPrimitive?.contentOrNull == "IOC" || effect == 1
            val cancelled = status in setOf("CANCELLED", "FAILED", "REJECTED") || ioc && partial
            if (status == "FILLED" && quantity.signum() == 0 && !cancelled) return null
            val fulfillment = when { requested == null -> "unknown"; quantity.signum() == 0 -> "none"; partial -> "partial"; else -> "full" }
            return OrderExecutionReceipt(orderId, status, value.filledSize,
                requestedSize = size?.takeIf { requested != null },
                remainingSize = requested?.takeIf { it >= quantity }?.subtract(quantity)?.toPlainString(),
                executionState = if (quantity.signum() == 0) { if (status in setOf("FAILED", "REJECTED")) "rejected" else "no_fill" } else if (partial || cancelled) "partial" else "filled",
                fulfillmentState = fulfillment,
                remainingDisposition = if (cancelled) "cancelled" else if (fulfillment == "full") "filled" else "unknown",
                avgFillPrice = value.avgFillPrice?.takeIf { decimal(it) != null },
                objectId = objectId, operationId = operation.id.value,
            )
        }
    }
}

@Serializable
public data class OrderExecutionUpdate(public val order: Value, public val fillsComplete: Boolean? = null) {
    @Serializable
    public data class Value(public val id: String? = null, public val orderId: String? = null, public val status: String, public val filledSize: String, public val avgFillPrice: String? = null)
}
