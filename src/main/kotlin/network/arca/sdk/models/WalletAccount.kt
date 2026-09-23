package network.arca.sdk.models

import kotlinx.serialization.Serializable

// The Wallet Account read model (documents/contracts/v9-cash-wallet-integration.md,
// "Wallet Account read model"): one owner-facing wallet — a V9 cash boundary
// plus the external address linked to it — composed by Arca from durable
// records with no chain call. Every money figure is a micro-USDC integer
// string; every figure carries the as-of it was recorded under. Enum values
// are the closed vocabulary shipped as
// backend/libs/arca-go/cashv9/testdata/wallet-account/vocabulary.json and are
// kept as strings on the wire types so a value this SDK predates still
// decodes; the typed accessors return null for one it does not know.

/** The block a figure is valid through, with its hash and time when recorded. */
@Serializable
public data class WalletAsOf(
    val block: Long,
    val hash: String? = null,
    val time: String? = null,
)

/**
 * Six-decimal integer strings (micro-USDC). [reservedMicro] and
 * [pendingOutMicro] are the same figure seen from two sides (the ledger hold
 * and the owner's "money leaving").
 */
@Serializable
public data class WalletBalances(
    val confirmedMicro: String,
    val availableMicro: String,
    val reservedMicro: String,
    val pendingInMicro: String,
    val pendingOutMicro: String,
    val asOf: WalletAsOf,
)

/**
 * The external address linked to the wallet, as the address observer
 * projects it. [balanceMicro] is null when the projection has no baseline —
 * never "0" as a stand-in for unknown.
 */
@Serializable
public data class WalletSource(
    val address: String,
    val balanceMicro: String? = null,
    val health: String,
    val completeThroughBlock: Long,
    val asOf: WalletAsOf,
)

/**
 * The automatic-deposit route and USDC allowance of the linked source on the
 * adapter, as observed on chain. [routeId] is absent when [state] is `off`;
 * [allowanceMicro] is absent only when no baseline has been read.
 */
@Serializable
public data class WalletAutoDeposit(
    val state: String,
    val routeId: String? = null,
    val allowanceMicro: String? = null,
    val asOf: WalletAsOf,
) {
    /** The typed state, or null for a value this SDK does not know. */
    val typedState: AutoDepositState? get() = AutoDepositState.fromWire(state)
}

/** One owner-started operation in the closed vocabulary. */
@Serializable
public data class WalletOperation(
    val id: String,
    val kind: String,
    val state: String,
    val reason: String? = null,
    val amountMicro: String,
    val destination: String? = null,
    val txHash: String? = null,
    val startedAt: String,
    val updatedAt: String,
    val canRetry: Boolean,
) {
    val typedState: WalletOperationState? get() = WalletOperationState.fromWire(state)
    val typedReason: WalletFailureReason? get() = reason?.let(WalletFailureReason::fromWire)
}

/**
 * The composed read model. [revision] is the realm's change-journal sequence
 * at composition; the stream resumes from it.
 */
@Serializable
public data class WalletAccount(
    val schema: Int,
    val revision: Long,
    val realmId: String,
    val boundaryId: String,
    val ownerAddress: String,
    val depositAddress: String? = null,
    val source: WalletSource? = null,
    val walletState: String,
    val attention: List<String>,
    val balances: WalletBalances,
    val autoDeposit: WalletAutoDeposit? = null,
    val operations: List<WalletOperation>,
    /**
     * Action-proposal attempts awaiting the owner's answer, soonest deadline
     * first. Null from servers that predate them.
     */
    val requirements: List<WalletRequirement>? = null,
    /**
     * Explicit deposit links into this boundary; with an active one,
     * [source] and [autoDeposit] describe its source wallet. Null when none.
     */
    val depositLinks: List<WalletDepositLink>? = null,
) {
    val typedWalletState: WalletState? get() = WalletState.fromWire(walletState)
}

/**
 * One open action-proposal attempt on the account. [expiresAt] is the signed
 * deadline in unix seconds; past it the attempt cannot be accepted. Read the
 * proposal for its payload.
 */
@Serializable
public data class WalletRequirement(
    val proposalId: String,
    val requirementId: String,
    val attemptId: String,
    val actionKind: String,
    val schemaId: String,
    val variant: String,
    val state: String,
    val expiresAt: Long,
)

/** `walletState` values. */
public enum class WalletState(public val wire: String) {
    SETUP_REQUIRED("setup_required"),
    SETTING_UP("setting_up"),
    READY("ready"),
    NEEDS_ATTENTION("needs_attention"),
    UNAVAILABLE("unavailable");

    public companion object {
        public fun fromWire(value: String): WalletState? = entries.firstOrNull { it.wire == value }
    }
}

/**
 * `operations[].state` values. [AWAITING_APPROVAL] and [UNCERTAIN] are
 * product-layer states Arca never produces itself; they are in the
 * vocabulary so every renderer shares one closed set.
 */
public enum class WalletOperationState(public val wire: String) {
    AWAITING_APPROVAL("awaiting_approval"),
    SENDING("sending"),
    CONFIRMING("confirming"),
    COMPLETED("completed"),
    FAILED("failed"),
    UNCERTAIN("uncertain");

    public companion object {
        public fun fromWire(value: String): WalletOperationState? = entries.firstOrNull { it.wire == value }
    }
}

/** `operations[].reason` values, present only when the state is `failed`. */
public enum class WalletFailureReason(public val wire: String) {
    CONSENT_EXPIRED("consent_expired"),
    REVERTED("reverted"),
    REJECTED("rejected"),
    DESTINATION_REFUSED("destination_refused"),
    RECOVERY_STARTED("recovery_started"),
    CANCELLED("cancelled"),
    UNKNOWN("unknown");

    public companion object {
        public fun fromWire(value: String): WalletFailureReason? = entries.firstOrNull { it.wire == value }
    }
}

/** `autoDeposit.state` values. */
public enum class AutoDepositState(public val wire: String) {
    OFF("off"),
    ACTIVE("active"),
    NEEDS_APPROVAL("needs_approval"),
    NEEDS_ATTENTION("needs_attention");

    public companion object {
        public fun fromWire(value: String): AutoDepositState? = entries.firstOrNull { it.wire == value }
    }
}
