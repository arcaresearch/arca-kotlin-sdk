package network.arca.sdk

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.ConnectionStatus
import network.arca.sdk.models.ProjectedValuation
import network.arca.sdk.models.ProjectionValuationsPage
import network.arca.sdk.models.ProjectionWindow
import network.arca.sdk.models.RealmProjection
import network.arca.sdk.models.revalued
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

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
 *
 * With [window], rows carry the windowed `pnl` and `volume` figures the
 * projection grants, each with its own `from` and status. A pending figure
 * has no value, so it never reads as zero.
 */
public suspend fun Arca.getProjectionValuations(
    name: String,
    prefix: String? = null,
    path: String? = null,
    paths: List<String>? = null,
    cursor: String? = null,
    limit: Int? = null,
    window: ProjectionWindow? = null,
): ProjectionValuationsPage {
    val query = buildMap {
        put("realmId", realm)
        prefix?.let { put("prefix", it) }
        path?.let { put("path", it) }
        paths?.takeIf { it.isNotEmpty() }?.let { put("paths", it.joinToString(",")) }
        cursor?.let { put("cursor", it) }
        limit?.let { put("limit", it.toString()) }
        window?.let { put("window", it.wire) }
    }
    return client.get("/projections/$name/valuations", query = query)
}

// MARK: - watchProjection

private const val PROJECTION_RECREATE_MAX_DELAY_MS = 30_000L

/**
 * Subscribe to a registered projection for a chosen set of accounts: one
 * batched watch delivering per-object REDACTED valuation deltas keyed by
 * path, for the [paths] (max 500) and/or [prefix] you name. Between server
 * deltas, rows re-mark client-side against the mids feed — and the stream
 * subscribes to mids only for the markets its rows hold positions in — so a
 * leaderboard ticks with the market at the cost of its own rows, not the
 * realm's.
 *
 * The scope only narrows the projection's registered resources: a path
 * outside them is dropped, a prefix is intersected with them. No scope at
 * all is an EMPTY watch that delivers nothing until
 * [ProjectionWatchStream.setPaths] names one; it never means every covered
 * account. Change the roster with `setPaths` / `setPrefix` without
 * re-subscribing.
 *
 * A path set comes back whole in the watch reply. A prefix scope is
 * paginated: the reply carries the first page and this method fetches the
 * rest over REST before the stream connects, so
 * [ProjectionWatchStream.valuations] starts complete.
 *
 * Server frames are deltas: only changed rows arrive, and deleted objects are
 * named in a `removed` list. The stream merges both into its map — consumers
 * always see the full current picture. The server-side watch dies with its
 * connection; re-creation on reconnect and rotation, with the current scope,
 * is automatic and retries with backoff until it succeeds. A delivery gap or
 * server resync re-reads the scope under the same watch. While either is
 * under way [ProjectionWatchStream.isStale] is true and the rows are the last
 * ones delivered. Call [ProjectionWatchStream.stop] when done.
 *
 * With [window], rows carry `pnl` and `volume`, each with its own `from` and
 * status. Rank or sum rows per figure only when their `from` matches.
 * `setPaths` / `setPrefix` keep the window.
 */
public suspend fun Arca.watchProjection(
    name: String,
    paths: List<String> = emptyList(),
    prefix: String? = null,
    exchange: String = "sim",
    window: ProjectionWindow? = null,
): ProjectionWatchStream {
    ws.ensureConnected()
    val initialScope = ProjectionWatchScope(paths, prefix)
    val windowWire = window?.wire

    suspend fun fetchAllValuations(reply: ProjectionWatchCreated, scope: ProjectionWatchScope): List<ProjectedValuation> {
        val all = reply.valuations.toMutableList()
        var next = reply.cursor
        while (next != null) {
            val page = getProjectionValuations(name, prefix = scope.prefix?.takeIf { it.isNotEmpty() }, cursor = next, window = window)
            all += page.valuations
            next = page.cursor
        }
        return all
    }

    val created = ws.createProjectionWatch(name, initialScope, windowWire)
    val initial = fetchAllValuations(created, initialScope).associateBy { it.path }

    val stream = ProjectionWatchStream(name, created.fields, created.window ?: windowWire)
    val structural = MutableStateFlow(initial)
    val mids = MutableStateFlow<Map<String, String>>(emptyMap())
    val widBox = MutableStateFlow(created.watchId)
    val scopeBox = MutableStateFlow(initialScope)
    val stopped = AtomicBoolean(false)
    val refreshing = AtomicBoolean(false)
    val resyncing = AtomicBoolean(false)
    val resyncAgain = AtomicBoolean(false)
    val jobs = mutableListOf<Job>()

    stream.watchIdMut.value = created.watchId
    stream.scopeMut.value = initialScope

    // The stream's mids interest is the union of its rows' position
    // markets: the manager subscribes the Arca socket to that union (and
    // the direct provider, when preferred), never to every market.
    val priceInterest = ws.registerPriceMarkets()
    val interestRevision = AtomicLong(0)
    fun syncInterest() {
        val markets = structural.value.values.flatMap { v -> v.positions.orEmpty().map { it.market } }.toSet()
        ws.updatePriceMarkets(priceInterest, markets, interestRevision.incrementAndGet())
    }
    syncInterest()

    fun emit() {
        val cur = mids.value
        val struct = structural.value
        stream.push(if (cur.isEmpty()) struct else struct.mapValues { (_, v) -> v.revalued(cur) })
    }

    fun replaceRows(vals: List<ProjectedValuation>) {
        structural.value = vals.associateBy { it.path }
        syncInterest()
        emit()
    }

    // The watch lives on the server side of a specific connection, so whenever
    // that connection is replaced it has to be re-created against the new one,
    // with the CURRENT scope. A failed re-create keeps the last rows, leaves
    // the stream reconnecting, and retries with backoff (1s doubling to 30s)
    // until one succeeds or the stream stops.
    suspend fun recreateWatch() {
        if (stopped.get() || !refreshing.compareAndSet(false, true)) return
        try {
            var attempt = 0
            while (!stopped.get()) {
                try {
                    val oldWatchId = widBox.value
                    val scope = scopeBox.value
                    val newWatch = ws.createProjectionWatch(name, scope, windowWire)
                    val vals = fetchAllValuations(newWatch, scope)
                    if (stopped.get()) {
                        ws.destroyProjectionWatch(newWatch.watchId)
                        return
                    }
                    widBox.value = newWatch.watchId
                    stream.watchIdMut.value = newWatch.watchId
                    // Best-effort: the server ignores unknown watch IDs, so this
                    // is safe even when the old watch died with the old socket.
                    ws.destroyProjectionWatch(oldWatchId)
                    replaceRows(vals)
                    stream.setState(WatchStreamState.CONNECTED)
                    return
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    log.warning("watch", e, mapOf("projection" to name, "attempt" to (attempt + 1).toString())) {
                        "projection watch re-create failed; retrying"
                    }
                    stream.setState(WatchStreamState.RECONNECTING)
                    delay(minOf(PROJECTION_RECREATE_MAX_DELAY_MS, 1_000L shl minOf(attempt, 5)))
                    attempt += 1
                }
            }
        } finally {
            refreshing.set(false)
        }
    }

    jobs += scope.launch {
        ws.statusStream.collect { s ->
            if (s == ConnectionStatus.DISCONNECTED && stream.state.value != WatchStreamState.LOADING) {
                stream.setState(WatchStreamState.RECONNECTING)
            } else if (s == ConnectionStatus.CONNECTED && stream.state.value == WatchStreamState.RECONNECTING) {
                launch { recreateWatch() }
            }
        }
    }

    // A rotation swaps the socket without an outage, so no status change fires
    // and the branch above never runs — but the watch still died with the
    // connection that retired; without this the stream goes permanently quiet.
    jobs += scope.launch {
        ws.rotatedStream.collect { launch { recreateWatch() } }
    }

    ws.acquireMids(exchange, allMarkets = false)

    jobs += scope.launch {
        ws.projectionValuationEvents().collect { evt ->
            if (evt.watchId != widBox.value) return@collect
            structural.update { cur ->
                val next = cur.toMutableMap()
                // Removals first: an account replaced at the same path arrives
                // as its new row, which a removal must not delete.
                evt.removed?.forEach { next.remove(it) }
                next.putAll(evt.valuations)
                next
            }
            if (!refreshing.get()) stream.setState(WatchStreamState.CONNECTED)
            syncInterest()
            emit()
        }
    }

    jobs += scope.launch {
        ws.midsEvents().collect { m ->
            mids.update { it + m }
            emit()
        }
    }

    // Scope changes are serialized so a later setPaths always wins; the
    // reply is the full snapshot of the new scope and replaces the rows.
    val scopeMutex = Mutex()
    stream.updateScopeAction = { newScope ->
        scopeMutex.withLock {
            if (!stopped.get()) {
                scopeBox.value = newScope
                stream.scopeMut.value = newScope
                val reply = ws.updateProjectionWatch(widBox.value, newScope)
                val vals = fetchAllValuations(reply, newScope)
                if (!stopped.get() && scopeBox.value == newScope) replaceRows(vals)
            }
        }
    }

    // object.valuation frames are critical: a lost one leaves a row wrong
    // until that account changes again. A delivery gap or a server resync
    // re-reads the scope under the same watch, behind any scope change in
    // flight; a reconnect re-reads anyway, so it is skipped then. If the
    // re-read fails the watch is re-created.
    fun resync() {
        if (stopped.get() || refreshing.get() || stream.state.value != WatchStreamState.CONNECTED) return
        if (!resyncing.compareAndSet(false, true)) {
            resyncAgain.set(true)
            return
        }
        stream.resyncingMut.value = true
        scope.launch {
            try {
                val reread = try {
                    do {
                        resyncAgain.set(false)
                        val scopeNow = scopeBox.value
                        scopeMutex.withLock {
                            if (!stopped.get() && scopeBox.value == scopeNow) {
                                val reply = ws.updateProjectionWatch(widBox.value, scopeNow)
                                val vals = fetchAllValuations(reply, scopeNow)
                                if (!stopped.get() && scopeBox.value == scopeNow) replaceRows(vals)
                            }
                        }
                    } while (resyncAgain.get() && !stopped.get())
                    true
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    log.warning("watch", e, mapOf("projection" to name)) {
                        "projection watch re-read after a gap failed; re-creating"
                    }
                    false
                }
                if (!reread) recreateWatch()
            } finally {
                resyncing.set(false)
                stream.resyncingMut.value = false
            }
        }
    }
    val gapHandler = ws.onGap { resync() }

    emit()
    stream.setState(WatchStreamState.CONNECTED)

    stream.stopAction = {
        stopped.set(true)
        ws.removeGapHandler(gapHandler)
        jobs.forEach { it.cancel() }
        ws.releasePriceMarkets(priceInterest)
        ws.releaseMids(allMarkets = false)
        ws.destroyProjectionWatch(widBox.value)
    }
    return stream
}
