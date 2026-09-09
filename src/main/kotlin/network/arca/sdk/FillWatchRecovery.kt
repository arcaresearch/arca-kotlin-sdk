package network.arca.sdk

import network.arca.sdk.models.Fill
import kotlinx.coroutines.ensureActive
import kotlin.coroutines.coroutineContext

/** Stable execution identity joins previews and recorded rows, never order ID. */
internal fun mergeWatchedFills(existing: List<Fill>, incoming: List<Fill>): List<Fill> {
    val all = existing + incoming
    val recorded = all.filter { !it.operationId.isNullOrEmpty() }.map { it.fillId ?: it.id }.toSet()
    val seen = HashSet<List<String?>>()
    return all.filter { fill ->
        val key = fill.fillId?.takeIf { it.isNotEmpty() } ?: fill.id
        val isRecorded = !fill.operationId.isNullOrEmpty()
        if (!isRecorded && key in recorded) false
        else seen.add(listOf(key, if (isRecorded) "recorded" else "preview", fill.orderId, fill.orderOperationId,
            fill.size, fill.price, fill.market, fill.side?.name))
    }.sortedByDescending { it.createdAt ?: "" }
}

/** A finite traversal. Repeated cursors fail instead of starting a polling loop. */
internal suspend fun Arca.fillWatchSnapshot(objectId: String, market: String?, limit: Int?): List<Fill> {
    var cursor: String? = null
    val seen = HashSet<String>()
    val fills = ArrayList<Fill>()
    repeat(1000) {
        coroutineContext.ensureActive()
        val page = listFills(objectId = objectId, market = market, limit = limit, cursor = cursor)
        fills.addAll(page.fills)
        val next = page.cursor?.takeIf { it.isNotEmpty() } ?: return mergeWatchedFills(emptyList(), fills)
        check(seen.add(next)) { "Repeated fill-history cursor" }
        cursor = next
    }
    error("Fill-history pagination exceeded 1000 pages")
}
