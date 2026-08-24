package network.arca.sdk

import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.ConnectionStatus
import network.arca.sdk.models.ProjectedValuation
import network.arca.sdk.models.ProjectionValuationsPage
import network.arca.sdk.models.RealmProjection
import network.arca.sdk.models.revalued
import java.util.concurrent.atomic.AtomicBoolean

// MARK: - Projection registry (CRUD)

@Serializable
internal data class UpsertProjectionRequest(
    val fields: List<String>,
    val resources: List<String>,
)

@Serializable
internal data class ProjectionListResponse(
    val projections: List<RealmProjection> = emptyList(),
)

/**
 * Create or replace a named projection on the realm: a field-filter over the
 * canonical object valuation shape (`equity`, `realizedValue`,
 * `unrealizedValue`, `positions`) plus the path patterns defining which
 * objects it covers. Requires `arca:ManageProjection`.
 *
 * Tokens granted `arca:ReadProjection` on the projection NAME can then
 * read/subscribe to the covered objects through the projection — and see only
 * the projected fields. Balances, reserved balances, and pending inbound are
 * never projectable.
 */
public suspend fun Arca.upsertProjection(
    name: String,
    fields: List<String>,
    resources: List<String>,
): RealmProjection {
    val body = arcaJson.encodeToJsonElement(
        UpsertProjectionRequest.serializer(),
        UpsertProjectionRequest(fields = fields, resources = resources),
    )
    return client.put("/realms/$realm/projections/$name", body = body)
}

/** Read one registered projection. Requires `arca:ManageProjection`. */
public suspend fun Arca.getProjection(name: String): RealmProjection =
    client.get("/realms/$realm/projections/$name")

/** List the realm's registered projections. Requires `arca:ManageProjection`. */
public suspend fun Arca.listProjections(): List<RealmProjection> =
    client.get<ProjectionListResponse>("/realms/$realm/projections").projections

/** Delete a registered projection. Requires `arca:ManageProjection`. Idempotent. */
public suspend fun Arca.deleteProjection(name: String) {
    client.delete<JsonElement>("/realms/$realm/projections/$name")
}

/**
 * Batched projection-scoped read: one page of redacted valuations for the
 * objects a registered projection covers, keyset-paginated by path. Requires
 * an `arca:ReadProjection` grant on the projection name.
 *
 * Rows carry only the projection's registered fields (plus identity:
 * `objectId`, `path`, `type`). Pass [cursor] from the prior page to continue;
 * [prefix] narrows to paths under a prefix (it can never widen the
 * projection); [path] is the exact-path single-object variant; [paths] reads a
 * known set (max 500) in one call, for when you already know which accounts
 * you want — a leaderboard roster — instead of paging the whole projection to
 * find them.
 */
public suspend fun Arca.getProjectionValuations(
    name: String,
    prefix: String? = null,
    path: String? = null,
    paths: List<String>? = null,
    cursor: String? = null,
    limit: Int? = null,
): ProjectionValuationsPage {
    val query = buildMap {
        put("realmId", realm)
        prefix?.let { put("prefix", it) }
        path?.let { put("path", it) }
        paths?.takeIf { it.isNotEmpty() }?.let { put("paths", it.joinToString(",")) }
        cursor?.let { put("cursor", it) }
        limit?.let { put("limit", it.toString()) }
    }
    return client.get("/projections/$name/valuations", query = query)
}

// MARK: - watchProjection

/**
 * Subscribe to a registered projection: a single batched watch covering every
 * object the projection's resources match, delivering per-object REDACTED
 * valuation deltas keyed by path. Between server deltas, rows re-mark
 * client-side against the mids feed — so a leaderboard ticks with the market
 * without any extra server traffic.
 *
 * The initial snapshot is paginated: the watch reply carries the first page
 * and this method fetches the remaining pages over REST before the stream
 * connects, so [ProjectionWatchStream.valuations] starts complete.
 *
 * Server frames are deltas: only changed rows arrive, and deleted objects are
 * named in a `removed` list. The stream merges both into its map — consumers
 * always see the full current picture. The server-side watch dies with its
 * connection; re-creation on reconnect and rotation is automatic. Call
 * [ProjectionWatchStream.stop] when done.
 */
public suspend fun Arca.watchProjection(name: String, exchange: String = "sim"): ProjectionWatchStream {
    ws.ensureConnected()

    suspend fun fetchAllValuations(created: ProjectionWatchCreated): List<ProjectedValuation> {
        val all = created.valuations.toMutableList()
        var next = created.cursor
        while (next != null) {
            val page = getProjectionValuations(name, cursor = next)
            all += page.valuations
            next = page.cursor
        }
        return all
    }

    val created = ws.createProjectionWatch(name)
    val initial = fetchAllValuations(created).associateBy { it.path }

    val stream = ProjectionWatchStream(name, created.fields)
    val structural = MutableStateFlow(initial)
    val mids = MutableStateFlow<Map<String, String>>(emptyMap())
    val widBox = MutableStateFlow(created.watchId)
    val stopped = AtomicBoolean(false)
    val refreshing = AtomicBoolean(false)
    val jobs = mutableListOf<Job>()

    stream.watchIdMut.value = created.watchId

    fun emit() {
        val cur = mids.value
        val struct = structural.value
        stream.push(if (cur.isEmpty()) struct else struct.mapValues { (_, v) -> v.revalued(cur) })
    }

    // The watch lives on the server side of a specific connection, so whenever
    // that connection is replaced it has to be re-created against the new one.
    // Returns false when the caller should leave stream state alone.
    suspend fun recreateWatch(): Boolean {
        if (stopped.get() || !refreshing.compareAndSet(false, true)) return false
        try {
            val oldWatchId = widBox.value
            val newWatch = ws.createProjectionWatch(name)
            val vals = fetchAllValuations(newWatch)
            if (stopped.get()) {
                ws.destroyProjectionWatch(newWatch.watchId)
                return false
            }
            widBox.value = newWatch.watchId
            stream.watchIdMut.value = newWatch.watchId
            ws.destroyProjectionWatch(oldWatchId)
            structural.value = vals.associateBy { it.path }
            emit()
        } catch (e: Throwable) {
            log.warning("watch", e, mapOf("projection" to name)) {
                "projection watch re-create failed (keeping existing data)"
            }
        } finally {
            refreshing.set(false)
        }
        return true
    }

    jobs += scope.launch {
        ws.statusStream.collect { s ->
            if (s == ConnectionStatus.DISCONNECTED && stream.state.value != WatchStreamState.LOADING) {
                stream.setState(WatchStreamState.RECONNECTING)
            } else if (s == ConnectionStatus.CONNECTED && stream.state.value == WatchStreamState.RECONNECTING) {
                if (!recreateWatch()) return@collect
                stream.setState(WatchStreamState.CONNECTED)
            }
        }
    }

    // A rotation swaps the socket without an outage, so no status change fires
    // and the branch above never runs — but the watch still died with the
    // connection that retired; without this the stream goes permanently quiet.
    jobs += scope.launch {
        ws.rotatedStream.collect { recreateWatch() }
    }

    ws.acquireMids(exchange)

    jobs += scope.launch {
        ws.projectionValuationEvents().collect { evt ->
            if (evt.watchId != widBox.value) return@collect
            structural.update { cur ->
                val next = cur.toMutableMap()
                next.putAll(evt.valuations)
                evt.removed?.forEach { next.remove(it) }
                next
            }
            stream.setState(WatchStreamState.CONNECTED)
            emit()
        }
    }

    jobs += scope.launch {
        ws.midsEvents().collect { m ->
            mids.update { it + m }
            emit()
        }
    }

    emit()
    stream.setState(WatchStreamState.CONNECTED)

    stream.stopAction = {
        stopped.set(true)
        jobs.forEach { it.cancel() }
        ws.releaseMids()
        ws.destroyProjectionWatch(widBox.value)
    }
    return stream
}
