package network.arca.sdk.models

import kotlinx.serialization.Serializable

/**
 * What an exchange arca reports about its own provisioning.
 *
 * Creating an exchange arca returns as soon as the object exists, which is
 * before its venue account does; until then, trading calls answer
 * `503 EXCHANGE_PROVISIONING`. [EventType.EXCHANGE_PROVISIONED] and
 * [EventType.EXCHANGE_READY] are how you learn that changed without polling for
 * it, and this is the payload both carry.
 */
@Serializable
public data class ExchangeProvisioning(
    public val objectId: String? = null,
    public val path: String? = null,
    /**
     * The boundary is cosign-armed, so the account exists but cannot trade
     * until the user co-signs the agent grant. Expect
     * [EventType.EXCHANGE_READY] once they do.
     */
    public val cosignRequired: Boolean? = null,
    /** The account can actually trade. Always true on [EventType.EXCHANGE_READY]. */
    public val tradable: Boolean? = null,
    /** The venue account address, once stamped. */
    public val accountAddress: String? = null,
    public val agentWalletId: String? = null,
)

/**
 * Money observed arriving at a watched deposit address.
 *
 * This is chain truth, not ledger truth: nothing here has been credited yet.
 * [amount] is the transfer's value in whole units, and [sweeping] says whether
 * the platform is already moving it into the boundary without further action
 * from the user.
 */
@Serializable
public data class DetectedDeposit(
    public val address: String? = null,
    public val from: String? = null,
    public val amount: String? = null,
    public val txHash: String? = null,
    public val block: Long? = null,
    public val boundaryId: String? = null,
    public val sweeping: Boolean? = null,
)
