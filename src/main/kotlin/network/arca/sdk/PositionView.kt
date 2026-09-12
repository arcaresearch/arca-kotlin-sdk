package network.arca.sdk

import java.math.BigDecimal
import java.util.UUID
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.serialization.json.*
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.*

/** Display facts only; projected rows carry no financial or trading-eligibility fields. */
public data class VisiblePosition(
    public val market: String,
    public val signedSize: String,
    public val source: String,
    public val authoritativePosition: SimPosition?,
)

/** Cumulative execution coverage, including operations that removed a row. */
public data class PositionExecutionCoverage(
    public val operationId: String,
    public val orderId: String,
    public val market: String,
    public val filledSize: String,
    public val status: String,
)

public data class PositionViewSnapshot(
    public val positions: List<VisiblePosition> = emptyList(),
    public val coverage: List<PositionExecutionCoverage> = emptyList(),
    public val pendingMarkets: List<String> = emptyList(),
    /** Absence of a row in these markets does NOT mean flat. */
    public val unavailableMarkets: List<String> = emptyList(),
)

/** Capture before submission; retain for retries of the SAME logical order. Not persistable. */
public class PositionUpdate internal constructor(
    internal val view: PositionView,
    public val market: String,
    public val side: OrderSide,
) {
    internal val id: UUID = UUID.randomUUID()
    /** Only when no order was dispatched, or the backend proves it never was. */
    public fun cancelBeforeSubmission() { view.cancel(this) }
    /** Attachment failed after dispatch: keep the order outcome, mark display unknown. */
    public fun invalidate() { view.invalidate(market) }
}

/** Shared account display. ExchangeState remains the financial authority. */
public class PositionView internal constructor(
    public val objectId: String,
    private val read: suspend () -> ExchangeState,
    private val launchRecovery: (suspend () -> Unit) -> Unit,
) {
    private val lock = Any()
    private val currentMut = MutableStateFlow(PositionViewSnapshot())
    public val current: StateFlow<PositionViewSnapshot> get() = currentMut.asStateFlow()
    private var state: ExchangeState? = null
    private var asOf: String? = null
    private var revision = 0L
    private var closed = false
    private data class Entry(val id: UUID, val market: String, val side: OrderSide, val baselineAsOf: String,
        var operationId: String? = null, var orderId: String? = null,
        var quantity: BigDecimal = BigDecimal.ZERO, var received: Boolean = false, var accounted: Boolean = false)
    private val entries = linkedMapOf<UUID, Entry>()
    private val baselines = linkedMapOf<String, BigDecimal>()
    private val unavailable = mutableSetOf<String>()
    private var completed = listOf<PositionExecutionCoverage>()
    private var reconciliationInFlight = false
    private val observedFills = mutableListOf<Triple<String, String?, String?>>()

    /** Requires a coherent dated account observation; call BEFORE the backend mutation. */
    public fun begin(market: String, side: OrderSide): PositionUpdate = synchronized(lock) {
        val base = state
        val expired = base?.tradingAllocation?.validUntil?.let {
            runCatching { !java.time.Instant.parse(it).isAfter(java.time.Instant.now()) }.getOrDefault(true)
        } ?: false
        check(!closed && market.isNotEmpty() && base != null && asOf != null && !expired && entries.size < 128 && market !in ambiguousMarkets()) { "POSITION_BASELINE_UNAVAILABLE" }
        if (market !in baselines) baselines[market] = size(base, market) ?: error("POSITION_BASELINE_UNAVAILABLE")
        val token = PositionUpdate(this, market, side)
        entries[token.id] = Entry(token.id, market, side, asOf!!)
        revision++; publish(); token
    }

    internal fun bind(token: PositionUpdate, operation: Operation, account: String) = synchronized(lock) {
        val entry = entries[token.id]
        val input = runCatching { operation.input?.let { arcaJson.parseToJsonElement(it).jsonObject } }.getOrNull()
        check(account == objectId && entry != null && operation.type == OperationType.ORDER &&
            timestamp(operation.createdAt)?.let { it >= entry.baselineAsOf } == true &&
            input?.get("exchangeObjectId")?.jsonPrimitive?.contentOrNull == account &&
            input["market"]?.jsonPrimitive?.contentOrNull == token.market && input["side"]?.jsonPrimitive?.contentOrNull == token.side.wire &&
            (entry.operationId == null || entry.operationId == operation.id.value) &&
            completed.none { it.operationId == operation.id.value } &&
            entries.values.none { it.id != token.id && it.operationId == operation.id.value }) { "POSITION_UPDATE_IDENTITY_MISMATCH" }
        entry.operationId = operation.id.value
        revision++; publish()
    }

    internal fun receive(token: PositionUpdate, receipt: OrderExecutionReceipt): Unit = synchronized(lock) {
        val entry = entries[token.id] ?: return
        val quantity = decimal(receipt.filledSize) ?: return
        if (closed || receipt.objectId != objectId || entry.operationId != receipt.operationId ||
            (entry.orderId != null && entry.orderId != receipt.orderId) || receipt.orderId.isEmpty() ||
            receipt.status.uppercase() !in setOf("FILLED", "CANCELLED", "CANCELED", "EXPIRED", "FAILED", "REJECTED") || quantity < entry.quantity) return
        if (entry.received && quantity.compareTo(entry.quantity) == 0) return
        entry.orderId = receipt.orderId; entry.quantity = quantity; entry.received = true; entry.accounted = false
        revision++; publish()
    }

    internal suspend fun accounted(token: PositionUpdate, detail: SimOrderWithFills? = null) {
        val ticket = synchronized(lock) {
            val entry = entries[token.id]
            if (closed || entry == null || !entry.received) return
            if (detail != null && (detail.order.id.value != entry.orderId || decimal(detail.order.filledSize)?.compareTo(entry.quantity) != 0)) return
            if (!entry.accounted) { entry.accounted = true; revision++ }
            if (entries.values.all { it.accounted } && !reconciliationInFlight) { reconciliationInFlight = true; revision } else null
        } ?: return
        recover(ticket)
    }

    private suspend fun recover(ticket: Long) {
        var next: Long? = ticket
        while (next != null) {
            val current = next
            try { reconcile(read(), current) }
            catch (e: kotlinx.coroutines.CancellationException) { synchronized(lock) { reconciliationInFlight = false }; throw e }
            catch (_: Exception) { /* Next observation retries a failed read. */ }
            next = synchronized(lock) {
                reconciliationInFlight = false
                if (!closed && entries.isNotEmpty() && revision != current && entries.values.all { it.accounted }) {
                    reconciliationInFlight = true; revision
                } else null
            }
        }
    }

    internal fun reconcile(fresh: ExchangeState, ticket: Long): Unit = synchronized(lock) {
        val stamp = timestamp(fresh) ?: return
        if (closed || revision != ticket || !entries.values.all { it.accounted } || (asOf != null && stamp < asOf!!)) return
        completed = (completed + coverage("accounted")).takeLast(128)
        entries.clear(); baselines.clear(); unavailable.clear(); observedFills.clear()
        state = fresh; asOf = stamp; revision++; publish()
    }

    internal fun observe(fresh: ExchangeState): Unit = synchronized(lock) {
        if (closed) return
        val stamp = timestamp(fresh)
        if (asOf != null && (stamp == null || stamp < asOf!!)) return
        val changed = stamp != asOf
        state = fresh; asOf = stamp; publish()
        if (changed && entries.isNotEmpty() && entries.values.all { it.accounted } && !reconciliationInFlight) {
            reconciliationInFlight = true
            val ticket = revision
            launchRecovery { recover(ticket) }
        }
    }

    internal fun observeFill(market: String, operationId: String?, orderId: String?, recordedAt: String? = null): Unit = synchronized(lock) {
        if (market !in baselines) return
        val committed = timestamp(recordedAt)
        val baseline = entries.values.filter { it.market == market }.minOfOrNull { it.baselineAsOf }
        if (committed != null && baseline != null && committed <= baseline) return
        val evidence = Triple(market, operationId, orderId)
        if (evidence in observedFills) return
        if (observedFills.size >= 256) unavailable.addAll(baselines.keys) else observedFills.add(evidence)
        publish()
    }

    internal fun invalidate(market: String? = null): Unit = synchronized(lock) {
        if (market == null) unavailable.addAll(baselines.keys) else if (market in baselines) unavailable.add(market)
        publish()
    }

    private fun ambiguousMarkets(): Set<String> {
        val result = unavailable.toMutableSet()
        for ((market, operation, order) in observedFills) {
            val matched = entries.values.any { it.market == market && ((operation != null && it.operationId == operation) || (order != null && it.orderId == order)) }
            if (!matched) result.add(market)
        }
        for ((market, baseline) in baselines) {
            val local = entries.values.filter { it.market == market }
            if (!local.all { it.received }) continue
            val observed = state?.let { size(it, market) }
            val low = baseline - local.filter { it.side == OrderSide.SELL }.sumOf { it.quantity }
            val high = baseline + local.filter { it.side == OrderSide.BUY }.sumOf { it.quantity }
            if (observed == null || observed < low || observed > high) result.add(market)
        }
        return result
    }

    private fun coverage(status: String? = null): List<PositionExecutionCoverage> {
        val ambiguous = ambiguousMarkets()
        return entries.values.mapNotNull { e ->
            if (!e.received || e.operationId == null || e.orderId == null) null else
                PositionExecutionCoverage(e.operationId!!, e.orderId!!, e.market, text(e.quantity), status ?: if (e.market in ambiguous) "unavailable" else "execution")
        }.sortedBy { it.operationId }
    }

    internal fun cancel(token: PositionUpdate): Unit = synchronized(lock) {
        val entry = entries[token.id] ?: return
        if (entry.operationId != null || entry.received) return
        entries.remove(token.id)
        if (entries.values.none { it.market == token.market }) { baselines.remove(token.market); unavailable.remove(token.market) }
        revision++; publish()
    }

    /** Retire on logout/account reset. Old handles and reads become inert. */
    public fun reset(): Unit = synchronized(lock) {
        closed = true; state = null; asOf = null; entries.clear(); baselines.clear(); observedFills.clear(); unavailable.clear(); completed = emptyList()
        revision++; publish()
    }

    private fun publish() {
        val ambiguous = ambiguousMarkets().toMutableSet()
        val rows = linkedMapOf<String, VisiblePosition>()
        for (position in state?.positions.orEmpty()) {
            val size = signed(position) ?: continue
            if (size.signum() != 0) rows[position.market] = VisiblePosition(position.market, text(size), "authoritative", position)
        }
        for ((market, baseline) in baselines) {
            rows.remove(market)
            if (market in ambiguous) continue
            var total = baseline
            for (entry in entries.values.filter { it.market == market }) total += if (entry.side == OrderSide.BUY) entry.quantity else -entry.quantity
            if (total.stripTrailingZeros().precision() > 38) { unavailable.add(market); ambiguous.add(market); continue }
            if (total.signum() != 0) rows[market] = VisiblePosition(market, text(total), if (entries.values.any { it.market == market && it.received }) "execution" else "baseline", null)
        }
        currentMut.value = PositionViewSnapshot(rows.values.sortedBy { it.market }, completed + coverage(), baselines.keys.sorted(), ambiguous.sorted())
    }

    internal companion object {
        fun timestamp(state: ExchangeState): String? = timestamp(state.tradingAllocation?.asOf)
        private fun timestamp(value: String?): String? {
            val raw = value ?: return null
            if (!raw.matches(Regex("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,9})?Z$"))) return null
            val parts = raw.dropLast(1).split('.')
            return parts[0] + "." + (parts.getOrNull(1) ?: "").padEnd(9, '0') + "Z"
        }
        fun decimal(raw: String): BigDecimal? = raw.takeIf { it.matches(Regex("^[0-9]+(?:\\.[0-9]+)?$")) && (it.substringAfter('.', "").length <= 38) && it.filter { c -> c != '.' }.trimStart('0').length <= 38 }?.toBigDecimalOrNull()
        private fun signed(position: SimPosition): BigDecimal? = decimal(position.size)?.let { if (position.side == PositionSide.LONG) it else -it }
        private fun size(state: ExchangeState, market: String): BigDecimal? {
            val rows = state.positions.filter { it.market == market }
            return if (rows.isEmpty()) BigDecimal.ZERO else if (rows.size == 1) signed(rows.first()) else null
        }
        private fun text(value: BigDecimal): String = value.stripTrailingZeros().toPlainString()
    }
}
