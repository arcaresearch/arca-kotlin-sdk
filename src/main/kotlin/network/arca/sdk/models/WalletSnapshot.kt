package network.arca.sdk.models

import kotlinx.serialization.Serializable

// The combined Cash and trading wallet snapshot, schema 2
// (documents/contracts/v9-wallet-snapshot.md): Cash, every trading account
// under the owner's root, the money between them and the funding operations
// that move it, composed by Arca from stored records in one read-only
// transaction at one journal position. Every money figure is a micro-USDC
// integer string. In-flight money is listed in `moving`, never hidden, and
// `totals.totalMicro = cashMicro + movingMicro + tradingMicro` at every
// position. Vocabulary values are kept as strings on the wire types so a value
// this SDK predates still decodes; the typed accessors return null for one it
// does not know.

/** The wallet snapshot schema this SDK reads. */
public const val WALLET_SNAPSHOT_SCHEMA: Int = 2

/**
 * What the snapshot's figures describe. [sequence] is the realm journal
 * position the snapshot was read at (a stream resumes after it); [revision]
 * changes only when one of the wallet's own boundaries changed.
 */
@Serializable
public data class WalletSnapshotWatermark(
    val sequence: Long = 0,
    val revision: Long = 0,
    val cashBlock: Long = 0,
    val cashBlockHash: String? = null,
    val nativeObservedAt: Long = 0,
    val settlementSequence: Long = 0,
    val lastSettlementBlock: Long = 0,
)

/**
 * A trading account's observed figures: [totalMicro] is the perp account
 * value plus USDC spot, [withdrawableMicro] what the venue would release.
 * [source] says where they came from: `hyperliquid_ws` (a venue push),
 * `arca_settlement` (what Arca booked, until the next push confirms it),
 * `arca_setup` (an account created by an unfinished move), `declared` or
 * `not_deployed` (zero by construction).
 */
@Serializable
public data class WalletNativeBalances(
    val totalMicro: String,
    val perpMicro: String = "0",
    val withdrawableMicro: String = "0",
    val spotMicro: String = "0",
    val source: String = "",
) {
    val typedSource: WalletNativeBalanceSource? get() = WalletNativeBalanceSource.fromWire(source)
}

/** `balances.source` values of a trading account. */
public enum class WalletNativeBalanceSource(public val wire: String) {
    HYPERLIQUID_WS("hyperliquid_ws"),
    ARCA_SETTLEMENT("arca_settlement"),
    ARCA_SETUP("arca_setup"),
    DECLARED("declared"),
    NOT_DEPLOYED("not_deployed");

    public companion object {
        public fun fromWire(value: String): WalletNativeBalanceSource? = entries.firstOrNull { it.wire == value }
    }
}

/**
 * One end of a movement: Cash (`kind` `cash`), a trading account (its
 * [arcaId], [arcaPath] and [boundaryId]), or an outside address (`kind`
 * `external`, or a provider name for an announced deposit, with the payer's
 * [address] once observed).
 */
@Serializable
public data class WalletEndpoint(
    val kind: String = "",
    val arcaId: String? = null,
    val arcaPath: String? = null,
    val boundaryId: String? = null,
    val address: String? = null,
) {
    /** Whether this end is the wallet's Cash account. */
    val isCash: Boolean get() = kind == "cash"

    internal val arcaIdOrNull: String? get() = arcaId?.takeIf { it.isNotEmpty() }
    internal val boundaryIdOrNull: String? get() = boundaryId?.takeIf { it.isNotEmpty() }
    internal val addressOrNull: String? get() = address?.takeIf { it.isNotEmpty() }
}

/**
 * Money in flight for one unfinished operation. [leg] is `reserved` (still
 * inside its source's figure), `in_transit` (left its source, not yet at its
 * destination; counted in `totals.movingMicro`) or `expected` (a deposit a
 * provider confirmed that is not on chain yet; listed, never summed).
 * [amountMicro] is what arrives.
 */
@Serializable
public data class WalletMoving(
    val operationId: String,
    val kind: String = "",
    val leg: String,
    val amountMicro: String,
    val from: WalletEndpoint = WalletEndpoint(),
    val to: WalletEndpoint = WalletEndpoint(),
) {
    val typedLeg: WalletMovingLeg? get() = WalletMovingLeg.fromWire(leg)
}

/** `moving[].leg` values. */
public enum class WalletMovingLeg(public val wire: String) {
    RESERVED("reserved"),
    IN_TRANSIT("in_transit"),
    EXPECTED("expected");

    public companion object {
        public fun fromWire(value: String): WalletMovingLeg? = entries.firstOrNull { it.wire == value }
    }
}

/** An account's own key-free receiving address; [pendingMicro] arrived there and is not swept yet. */
@Serializable
public data class WalletDepositAddress(
    val address: String,
    val lifecycle: String = "",
    val pendingMicro: String = "0",
)

/** How far a trading account is from placing orders, from stored records. */
@Serializable
public data class WalletTradingReadiness(
    val state: String = "unavailable",
    val bindingOperationId: String? = null,
) {
    val typedState: WalletTradingReadinessState? get() = WalletTradingReadinessState.fromWire(state)
}

/** `trading.state` values of a trading account. */
public enum class WalletTradingReadinessState(public val wire: String) {
    SETUP_REQUIRED("setup_required"),
    TRADING_DISABLED("trading_disabled"),
    AWAITING_FIRST_CREDIT("awaiting_first_credit"),
    CONSENT_REQUIRED("consent_required"),
    CONSENTED("consented"),
    UNAVAILABLE("unavailable");

    public companion object {
        public fun fromWire(value: String): WalletTradingReadinessState? = entries.firstOrNull { it.wire == value }
    }
}

/**
 * The standing shape of one route on a trading account at this snapshot.
 * Advisory: a quote is the priced, expiring review; a preview never admits,
 * reserves or prices a specific amount.
 */
@Serializable
public data class WalletRoutePreview(
    val route: String = "",
    val allowed: Boolean = false,
    val reason: String? = null,
    val minimumRaw: String? = null,
    val maxRaw: String? = null,
    val activationFeeRaw: String? = null,
    val networkFeeRaw: String? = null,
    val requiresSetup: Boolean = false,
)

/** Whether an account may take part in one action, with the reason when not. */
@Serializable
public data class WalletAccountCapability(
    val allowed: Boolean = false,
    val reason: String? = null,
)

/**
 * One declared or deployed trading account under the owner's root.
 * [balances] is null only for a deployed account never observed; [asOf] is
 * the venue time the figures describe and [current] is false once they went
 * quiet. [heldBy] names the funding operation holding the account: while it
 * is set the figure keeps its pre-operation value and the money is listed in
 * `moving` instead, so a move is counted once.
 */
@Serializable
public data class WalletTradingAccount(
    val arcaId: String,
    val arcaPath: String = "",
    val boundaryId: String = "",
    val kind: String = "",
    val accountAddress: String = "",
    val localId: String? = null,
    val labels: Map<String, String> = emptyMap(),
    val state: String = "",
    val setupStatus: String = "",
    val lifecycle: String = "",
    val declaredAt: String? = null,
    val revision: Long = 0,
    val balances: WalletNativeBalances? = null,
    val asOf: String? = null,
    val current: Boolean = false,
    val heldBy: String? = null,
    val routes: List<WalletRoutePreview> = emptyList(),
    val trading: WalletTradingReadiness = WalletTradingReadiness(),
    val depositAddress: WalletDepositAddress? = null,
    val factsSequence: Long = 0,
    val setupRetryable: Boolean = false,
    val activation: String? = null,
    val fundingState: String? = null,
    val capabilities: Map<String, WalletAccountCapability> = emptyMap(),
    val deferredActivationFeeRaw: String? = null,
    val deferredActivationFeeState: String? = null,
    val deferredActivationFeeTrigger: String? = null,
) {
    val typedState: WalletTradingAccountState? get() = WalletTradingAccountState.fromWire(state)

    /** The owner-facing name from `labels.name`, null when unnamed. */
    val name: String? get() = labels["name"]?.takeIf { it.isNotEmpty() }

    /** The catalog position from `labels.ordinal`, null when unordered. */
    val ordinal: Int? get() = labels["ordinal"]?.toIntOrNull()

    /** [heldBy] as the operation holding the account, null when none. */
    val heldByOperation: String? get() = heldBy?.takeIf { it.isNotEmpty() }
}

/** `accounts[].state` values. */
public enum class WalletTradingAccountState(public val wire: String) {
    DECLARED("declared"),
    SETTING_UP("setting_up"),
    SETUP_RETRYABLE("setup_retryable"),
    READY("ready"),
    ARCHIVED("archived"),
    UNAVAILABLE("unavailable");

    public companion object {
        public fun fromWire(value: String): WalletTradingAccountState? = entries.firstOrNull { it.wire == value }
    }
}

/** The Cash policy-move leg of a Cash <-> trading move. */
@Serializable
public data class WalletCashLeg(
    val leg: String = "",
    val state: String = "",
    val amountMicro: String = "0",
    val txHash: String? = null,
)

/** One operator transaction of a funding operation and its state. */
@Serializable
public data class WalletMoveStep(
    val name: String = "",
    val state: String = "",
    val txHash: String? = null,
    val blockNumber: Long? = null,
    val blockHash: String? = null,
)

/** A send's current owner-signed consent. */
@Serializable
public data class WalletMoveConsent(
    val proposalId: String = "",
    val attempt: Int = 0,
    val expiresAt: Long = 0,
    val signed: Boolean = false,
)

/** An owner answer an operation waits for (`setup_signature`, `send_signature`). */
@Serializable
public data class WalletFundingRequirement(
    val kind: String = "",
    val operationId: String = "",
    val proposalId: String = "",
    val actionIds: List<String> = emptyList(),
    val expiresAt: Long = 0,
)

/** The settlement that proved a funding operation's charge and credit. */
@Serializable
public data class WalletSettlement(
    val sequence: Long = 0,
    val nativeTxHash: String? = null,
    val coreBlock: Long = 0,
    val coreBlockHash: String? = null,
    val amountMicro: String = "0",
    val feeMicro: String = "0",
    val creditMicro: String = "0",
    val final: Boolean = false,
)

/** Who sent a deposit, as the product announced it. */
@Serializable
public data class WalletDepositSource(
    val kind: String = "",
    val reference: String? = null,
)

/** The canonical transfer into an account's deposit address. */
@Serializable
public data class WalletDepositArrival(
    val txHash: String = "",
    val logIndex: Long = 0,
    val blockNumber: Long = 0,
    val blockHash: String = "",
    val from: String = "",
    val amountMicro: String = "0",
)

/**
 * One funding operation of the wallet with everything needed to render its
 * progress. [kind] is `setup`, `deposit`, `cash_to_trading`,
 * `trading_to_cash`, `trading_to_trading` or `trading_to_external`;
 * [debitBooked] and [creditBooked] say which side Arca has booked; once the
 * settlement proved the charge, [actualDebitMicro] and [actualNetworkFeeMicro]
 * are set together.
 */
@Serializable
public data class WalletFundingOperation(
    val operationId: String,
    val requestId: String? = null,
    val kind: String = "",
    val stage: String = "",
    val status: String = "",
    val attention: String? = null,
    val error: String? = null,
    val safeToReviewAgain: Boolean = false,
    val amountMicro: String = "0",
    val arrivesMicro: String = "0",
    val debitMicro: String = "0",
    val activationFeeMicro: String = "0",
    val networkFeeMicro: String = "0",
    val actualDebitMicro: String? = null,
    val actualNetworkFeeMicro: String? = null,
    val from: WalletEndpoint = WalletEndpoint(),
    val to: WalletEndpoint = WalletEndpoint(),
    val steps: List<WalletMoveStep> = emptyList(),
    val debitBooked: Boolean = false,
    val creditBooked: Boolean = false,
    val cashLeg: WalletCashLeg? = null,
    val setupOperationId: String? = null,
    val setupDeadline: Long? = null,
    val setupState: String? = null,
    val consent: WalletMoveConsent? = null,
    val requirements: List<WalletFundingRequirement> = emptyList(),
    val settlement: WalletSettlement? = null,
    val txHash: String? = null,
    val createdAt: String = "",
    val origin: String? = null,
    val source: WalletDepositSource? = null,
    val arrival: WalletDepositArrival? = null,
    val expiresAt: Long? = null,
    val sweepOperationId: String? = null,
    val deferredActivationFeeRaw: String? = null,
    val deferredActivationFeeState: String? = null,
    val deferredActivationFeeTrigger: String? = null,
) {
    /** Whether the operation has reached a terminal status. */
    val isTerminal: Boolean get() = status == "succeeded" || status == "failed" || status == "completed"

    /** The `requestId` as the move's create key, null when absent. */
    val requestIdOrNull: String? get() = requestId?.takeIf { it.isNotEmpty() }
}

/**
 * `totals.totalMicro = cashMicro + movingMicro + tradingMicro` at every
 * position. [complete] is false when a component has no figure; [current]
 * is false when any figure is not current.
 */
@Serializable
public data class WalletTotals(
    val cashMicro: String = "0",
    val movingMicro: String = "0",
    val tradingMicro: String = "0",
    val totalMicro: String,
    val complete: Boolean = false,
    val current: Boolean = false,
)

/** The whole-wallet accounting envelope; null amounts are unknown. */
@Serializable
public data class WalletAccounting(
    val revision: Long = 0,
    val sourceMicro: String? = null,
    val cashMicro: String = "0",
    val availableCashMicro: String = "0",
    val tradingMicro: String = "0",
    val movingMicro: String = "0",
    val totalMicro: String? = null,
    val complete: Boolean = false,
    val current: Boolean = false,
    val cashBlock: Long = 0,
    val cashBlockHash: String? = null,
)

/** The Cash allocation record moves debit and credit, present once Cash is set up. */
@Serializable
public data class WalletCashAccount(
    val arcaId: String = "",
    val defaultArcaId: String = "",
    val arcaPath: String = "",
    val localId: String = "",
    val accountStatus: String = "",
)

/**
 * The complete wallet at one journal position: Cash, every trading account,
 * money in flight and funding operations. Replace it as a whole; it is never
 * refused for movement in progress. A wallet exists from its funding source,
 * before Cash setup: such a pre-Cash wallet has an empty [boundaryId], Cash
 * in `setup_required` with zero balances, no accounts and nothing moving.
 */
@Serializable
public data class WalletSnapshot(
    val schema: Int = 0,
    val realmId: String = "",
    val ownerAddress: String = "",
    val arcaPath: String = "",
    val boundaryId: String = "",
    val cash: WalletAccount,
    val cashTarget: WalletEndpoint = WalletEndpoint(kind = "cash"),
    val cashAccount: WalletCashAccount? = null,
    val accounts: List<WalletTradingAccount> = emptyList(),
    val moving: List<WalletMoving> = emptyList(),
    val operations: List<WalletFundingOperation> = emptyList(),
    val totals: WalletTotals,
    val accounting: WalletAccounting? = null,
    val attention: List<String> = emptyList(),
    val watermark: WalletSnapshotWatermark = WalletSnapshotWatermark(),
    val composedAt: String = "",
) {
    /** The trading account with this `arcaId`, if the snapshot lists it. */
    public fun account(arcaId: String): WalletTradingAccount? = accounts.firstOrNull { it.arcaId == arcaId }

    /**
     * Whether the snapshot's figures still describe the wallet [previous]
     * does (same owner, root and realm) and are at least as new. Older or
     * foreign frames are ignored by the money watch.
     */
    public fun supersedes(previous: WalletSnapshot?): Boolean {
        if (previous == null) return true
        if (!previous.ownerAddress.equals(ownerAddress, ignoreCase = true) || previous.arcaPath != arcaPath || previous.realmId != realmId) return false
        return watermark.sequence >= previous.watermark.sequence
    }
}
