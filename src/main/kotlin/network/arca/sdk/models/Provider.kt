package network.arca.sdk.models

import kotlinx.serialization.Serializable

// Provider objects and deposit links. A provider object is an ordinary Arca
// object (type `provider`) naming one account at an external wallet provider —
// Privy today — at whatever path the product chooses. It holds no balance and
// grants no spending authority. A deposit link is a durable relationship from
// one of its wallets to one Cash account through the realm's automatic-deposit
// adapter.

@Serializable
public data class ProviderEvidenceFacts(
    val kind: String,
    val issuedAt: String,
    val expiresAt: String,
    val keyId: String? = null,
)

/** [status] is `unverified` or `verified`. */
@Serializable
public data class ProviderConnection(
    val status: String,
    val checkedAt: String? = null,
    val evidence: ProviderEvidenceFacts? = null,
)

@Serializable
public data class ProviderState(
    val schema: Int,
    val provider: String,
    val subject: String? = null,
    val connection: ProviderConnection,
)

/**
 * The realm's observation of one address. [balanceMicro] null means unknown,
 * never zero; a [health] other than `live` means it may be stale.
 */
@Serializable
public data class AddressObservationView(
    val watchId: String,
    val chainId: String,
    val tokenAddress: String,
    val balanceMicro: String? = null,
    val health: String,
    val completeThroughBlock: Long,
    val asOfBlockHash: String? = null,
    val asOfTime: String? = null,
)

@Serializable
public data class ProviderWalletControl(val target: String, val relation: String)

/** [status] is `verified`, or `unlinked` when the latest verification no longer lists it. */
@Serializable
public data class ProviderWalletVerification(
    val status: String,
    val verifiedAt: String? = null,
    val evidenceKind: String? = null,
    val evidenceIssuedAt: String? = null,
)

@Serializable
public data class ProviderWallet(
    val walletId: String,
    val chainType: String,
    val address: String,
    val walletClientType: String? = null,
    val providerWalletId: String? = null,
    val role: String? = null,
    val verification: ProviderWalletVerification,
    val controls: List<ProviderWalletControl> = emptyList(),
    val observation: AddressObservationView? = null,
)

@Serializable
public data class ProviderDetail(
    val objectId: String,
    val path: String,
    val state: ProviderState,
    val wallets: List<ProviderWallet> = emptyList(),
)

@Serializable
public data class DepositLinkAccountRef(val kernel: String, val venue: String, val localId: String)

@Serializable
public data class DepositLinkSource(val objectId: String, val path: String, val walletId: String, val address: String)

@Serializable
public data class DepositLinkDestination(
    val objectId: String,
    val path: String,
    val boundaryId: String,
    val account: DepositLinkAccountRef,
)

@Serializable
public data class DepositLinkAdapter(val address: String, val kind: String, val runtimeCodeHash: String? = null)

/** [state] is `active` or `revoked` — the application's request, not the chain's state. */
@Serializable
public data class DepositLinkRequested(
    val state: String,
    val at: String,
    val by: String? = null,
    val revokeRequestedAt: String? = null,
    val revokeRequestedBy: String? = null,
)

@Serializable
public data class DepositLinkConsent(
    val permissionVersion: String,
    val nonce: String,
    val allowanceRaw: String,
    val deadline: Long,
)

@Serializable
public data class DepositLinkLimits(val allowanceMicro: String? = null)

@Serializable
public data class DepositRouteAsOf(val block: Long, val hash: String? = null, val time: String? = null)

/**
 * [status]: unwatched, unknown (nothing known), off (known absent), mismatched
 * (forwards elsewhere), active, needs_approval, needs_attention.
 */
@Serializable
public data class DepositRouteObservation(
    val status: String,
    val routeId: String? = null,
    val account: String? = null,
    val localId: String? = null,
    val permissionVersion: Long? = null,
    val allowanceMicro: String? = null,
    val matchesDestination: Boolean = false,
    val health: String? = null,
    val asOf: DepositRouteAsOf? = null,
)

@Serializable
public data class DepositLinkOperations(val setupOperationId: String? = null, val revokeOperationId: String? = null)

/**
 * One link with requested, observed and accepted-operation state side by
 * side. [progress]: in_sync, setup_pending, setup_accepted, revoke_pending,
 * revoke_in_flight or unknown.
 */
@Serializable
public data class DepositLink(
    val id: String,
    val realmId: String,
    val source: DepositLinkSource,
    val destination: DepositLinkDestination,
    val chainId: String,
    val token: String,
    val adapter: DepositLinkAdapter,
    val requested: DepositLinkRequested,
    val lifetime: String,
    val consent: DepositLinkConsent? = null,
    val limits: DepositLinkLimits? = null,
    val observed: DepositRouteObservation,
    val operations: DepositLinkOperations = DepositLinkOperations(),
    val progress: String,
    val createdAt: String,
    val updatedAt: String,
)

/** One explicit deposit link into a Wallet Account's boundary. */
@Serializable
public data class WalletDepositLink(
    val linkId: String,
    val sourceObjectId: String,
    val sourceWalletId: String,
    val sourceAddress: String,
    val adapter: String,
    /** `active` or `revoked`. */
    val requestedState: String,
    /** A [DepositRouteObservation] status. */
    val observedStatus: String,
)
