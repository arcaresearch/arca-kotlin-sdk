package network.arca.sdk

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withContext
import kotlinx.serialization.SerializationException
import network.arca.sdk.internal.SseBackoff
import network.arca.sdk.internal.SseParser
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.WalletAccount
import okhttp3.Response
import java.io.IOException

// V9 cash: Wallet Account — one read and one stream per owner-facing wallet,
// composed by Arca from durable records with no chain call
// (documents/contracts/v9-cash-wallet-integration.md, "Wallet Account read
// model" and "Streams"). The realm is the one this client was initialised for.

private const val WALLET_ACCOUNT_PATH = "/custody/v9/cash/wallet-account"

/**
 * The Wallet Account for one boundary: confirmed, available, pending in,
 * pending out, `walletState`, the linked source, the automatic-deposit state
 * and every operation the owner started. Requires `arca:ReadObject` on the
 * boundary — a realm-scoped device token is enough. Throws
 * [ArcaException.NotFound] for an unknown boundary.
 */
public suspend fun Arca.walletAccount(boundaryId: String): WalletAccount =
    client.get(WALLET_ACCOUNT_PATH, mapOf("realmId" to realmId, "boundaryId" to boundaryId))

/**
 * The Wallet Account of the boundary an owner address controls (the active
 * one when the owner has several). Not found when it has none.
 */
public suspend fun Arca.walletAccountByOwner(ownerAddress: String): WalletAccount =
    client.get(WALLET_ACCOUNT_PATH, mapOf("realmId" to realmId, "ownerAddress" to ownerAddress))

/**
 * Stream the Wallet Account of one boundary: a complete snapshot on connect
 * and after every change to the boundary or to its linked address, coalesced
 * to at most one per 250 ms. Every element is the whole state — replace what
 * you show; a missed frame costs latency, never correctness.
 *
 * Resume is automatic: after a disconnect (the ingress resets connections
 * about hourly) the flow reconnects with `Last-Event-ID` set to the last
 * snapshot's `revision`, backing off 1 s → 30 s and resetting the backoff once
 * a snapshot is delivered, and the server answers with exactly one fresh
 * snapshot. A 401/403 is retried once with a refreshed credential when a token
 * provider is configured. The flow fails only when the server refuses the
 * connection (not found, forbidden, the per-key stream cap); cancelling the
 * collector ends it silently.
 *
 * ```kotlin
 * arca.walletAccountEvents(boundaryId).collect { wallet -> render(wallet) }
 * ```
 */
public fun Arca.walletAccountEvents(boundaryId: String): Flow<WalletAccount> = channelFlow {
    WalletAccountStreamRunner(client, realmId, boundaryId, log).run(this)
}

/** The reconnect loop behind [walletAccountEvents]. One instance per stream. */
internal class WalletAccountStreamRunner(
    private val client: ArcaClient,
    private val realm: String,
    private val boundaryId: String,
    private val log: ArcaLogger,
) {
    /** Revision of the last delivered snapshot, sent as `Last-Event-ID`. */
    var lastRevision: Long? = null
        private set

    private var deliveredSinceLastBackoffReset = false

    private sealed class Connection {
        /** The connection ended (EOF, network error); reconnect. */
        class Disconnected(val cause: Throwable?) : Connection()

        /** The server refused; retry once after a credential refresh. */
        class RefreshAndRetry(val trigger: AuthRefreshTrigger) : Connection()

        /** The server refused for good. */
        class Refused(val error: ArcaException) : Connection()
    }

    suspend fun run(scope: ProducerScope<WalletAccount>) {
        var attempt = 0
        var refreshedThisAttempt = false
        while (currentCoroutineContext().isActive) {
            when (val outcome = connect(scope)) {
                is Connection.Refused -> throw outcome.error
                is Connection.RefreshAndRetry -> {
                    if (refreshedThisAttempt || !client.canRefreshToken) {
                        throw if (outcome.trigger == AuthRefreshTrigger.FORBIDDEN) {
                            ArcaException.Forbidden("Wallet Account stream refused", null, "FORBIDDEN")
                        } else {
                            ArcaException.Unauthorized("Wallet Account stream refused", null)
                        }
                    }
                    client.refreshToken(outcome.trigger)
                    refreshedThisAttempt = true
                    // Reconnect immediately with the new credential.
                }
                is Connection.Disconnected -> {
                    // A connection that delivered a snapshot starts the
                    // schedule over; only consecutive failures climb it.
                    if (deliveredSinceLastBackoffReset) {
                        attempt = 0
                        deliveredSinceLastBackoffReset = false
                    }
                    val wait = SseBackoff.delayMillis(attempt)
                    log.notice(
                        "stream",
                        outcome.cause,
                        mapOf("boundaryId" to boundaryId, "lastRevision" to (lastRevision?.toString() ?: ""), "delayMillis" to wait.toString()),
                    ) { "wallet account stream disconnected; reconnecting" }
                    attempt += 1
                    refreshedThisAttempt = false
                    delay(wait)
                }
            }
        }
    }

    private suspend fun connect(scope: ProducerScope<WalletAccount>): Connection {
        val request = client.streamRequest(
            "/custody/v9/cash/wallet-account/events",
            mapOf("realmId" to realm, "boundaryId" to boundaryId),
            lastRevision?.toString(),
        )
        val call = client.newStreamCall(request)
        val response: Response = try {
            withContext(Dispatchers.IO) { call.await() }
        } catch (e: IOException) {
            return Connection.Disconnected(ArcaException.Network(e))
        }
        response.use { resp ->
            val body = resp.body ?: return Connection.Disconnected(null)
            if (resp.code != 200) {
                val text = runCatching { body.string() }.getOrDefault("")
                val refusal = client.streamRefusal(text, resp.code)
                return when {
                    refusal is ArcaException.Unauthorized -> Connection.RefreshAndRetry(AuthRefreshTrigger.UNAUTHORIZED)
                    refusal is ArcaException.Forbidden -> Connection.RefreshAndRetry(AuthRefreshTrigger.FORBIDDEN)
                    resp.code in 502..504 -> Connection.Disconnected(refusal) // the ingress, not the API's answer
                    else -> Connection.Refused(refusal)
                }
            }
            val parser = SseParser()
            val source = body.source()
            val buffer = ByteArray(8 * 1024)
            try {
                while (true) {
                    currentCoroutineContext().ensureActive()
                    // One blocking read per chunk on IO; parsing and emission
                    // happen on the collector's context.
                    val read = withContext(Dispatchers.IO) { source.read(buffer) }
                    if (read < 0) break
                    for (i in 0 until read) {
                        val frame = parser.consume(buffer[i].toInt() and 0xff) ?: continue
                        if (frame.event != null && frame.event != "snapshot") continue // unknown frames are never applied
                        val snapshot = try {
                            arcaJson.decodeFromString(WalletAccount.serializer(), frame.data)
                        } catch (e: SerializationException) {
                            return Connection.Refused(ArcaException.Decoding(e))
                        } catch (e: IllegalArgumentException) {
                            return Connection.Refused(ArcaException.Decoding(e))
                        }
                        if (snapshot.schema != 1) {
                            return Connection.Refused(
                                ArcaException.Unknown("SCHEMA_UNSUPPORTED", "Wallet Account schema ${snapshot.schema} is not supported by this SDK", null),
                            )
                        }
                        lastRevision = snapshot.revision
                        deliveredSinceLastBackoffReset = true
                        scope.send(snapshot)
                    }
                }
            } catch (e: IOException) {
                return Connection.Disconnected(ArcaException.Network(e))
            } finally {
                runCatching { call.cancel() }
            }
        }
        // A clean end of body is the ingress's periodic reset, never completion.
        return Connection.Disconnected(null)
    }
}
