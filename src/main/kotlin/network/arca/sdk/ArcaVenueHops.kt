package network.arca.sdk

import kotlinx.serialization.Serializable
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.Operation
import network.arca.sdk.models.TransferResponse

// MARK: - Venue-to-venue hops

/**
 * Signs a co-sign digest with the boundary's co-sign key.
 *
 * Receives the kernel-derived EIP-712 digest (0x-prefixed 32 bytes) and the
 * full proposal, and returns a 0x-prefixed 65-byte secp256k1 signature.
 * Suspend so it can be a device prompt, a hardware wallet, or a remote signer
 * — the SDK never holds this key, which is the whole property a co-signature
 * provides.
 *
 * [hopVenues] has already re-derived the digest from the proposal's own fields
 * by the time this is called, so the hash is verified rather than merely
 * relayed; the proposal is passed alongside so the signer can show the user
 * what it commits to.
 */
public typealias CosignDigestSigner = suspend (digest: String, proposal: VenueHopProposal) -> String

/** The EIP-712 domain a co-sign digest is bound to. */
@Serializable
public data class CosignDomain(
    public val name: String,
    public val version: String,
    public val chainId: Long,
    public val verifyingContract: String,
)

/**
 * Everything a signer needs to re-derive and sign a venue hop.
 *
 * [amountRaw] — not [amount] — is what the paramsHash commits to. Encoding the
 * decimal string produces a different hash than the kernel and the signature
 * is rejected.
 */
@Serializable
public data class VenueHopProposal(
    /** CosignAction discriminator (11 = TransferBetweenVenues). */
    public val action: Int = 0,
    /** The debited (source) boundary — the co-sign owner. */
    public val boundaryId: String = "",
    public val boundaryKey: String = "",
    /** The credited boundary. Equal to [boundaryId] for a same-boundary rebalance. */
    public val targetBoundaryId: String = "",
    public val targetBoundaryKey: String = "",
    /** Venue CONTRACT addresses the hop routes between; both are in the digest. */
    public val fromVenue: String = "",
    public val toVenue: String = "",
    /** Destination venue sub-account (bytes32). */
    public val toVenueAccountKey: String = "",
    public val token: String = "",
    public val domain: CosignDomain? = null,
    public val amount: String = "",
    /** The uint256 the paramsHash commits to. Encode THIS, never [amount]. */
    public val amountRaw: String = "",
    /** Correlation word bound into the digest; derived from realm + operation path. */
    public val ref: String = "",
    public val nonce: String = "",
    public val deadline: Long = 0,
    public val paramsHash: String = "",
    /** Read from the kernel, so a signer can cross-check its own derivation. */
    public val digest: String = "",
) {
    /**
     * Re-derives [paramsHash] and [digest] from this proposal's own semantic
     * fields and throws when they disagree with what the server returned.
     *
     * This is what makes a co-signature meaningful. Without it a signer is
     * attesting to a 32-byte number it cannot read; with it, a server that
     * returned a digest for a different destination, a different amount, or a
     * different kernel is caught before the key is ever used.
     *
     * [hopVenues] calls this automatically before invoking its signer, so the
     * common path is verified by default. Call it directly when you drive
     * [proposeVenueHop] yourself.
     *
     * @throws IllegalStateException if the proposal does not describe what it
     *   asks to have signed.
     */
    public fun verify() {
        val d = domain
            ?: error("arca: venue hop proposal carries no EIP-712 domain; there is nothing to verify it against")
        check(d.name == COSIGN_DOMAIN_NAME && d.version == COSIGN_DOMAIN_VERSION) {
            "arca: venue hop proposal is for EIP-712 domain \"${d.name}\" version \"${d.version}\", but this SDK " +
                "derives \"$COSIGN_DOMAIN_NAME\" version \"$COSIGN_DOMAIN_VERSION\" — the kernel's signing " +
                "contract has moved and this SDK cannot verify what it would be signing"
        }
        check(
            action == CosignAction.TRANSFER_BETWEEN_VENUES ||
                action == CosignAction.TRANSFER_BETWEEN_VENUES_PRE_AUTH,
        ) {
            "arca: venue hop proposal carries action $action, want " +
                "${CosignAction.TRANSFER_BETWEEN_VENUES} or ${CosignAction.TRANSFER_BETWEEN_VENUES_PRE_AUTH}"
        }

        val derivedParams = Cosign.transferBetweenVenuesParamsHash(
            fromVenue = fromVenue,
            fromBoundary = boundaryKey,
            toBoundary = targetBoundaryKey,
            toVenue = toVenue,
            toVenueAccountId = toVenueAccountKey,
            token = token,
            amount = amountRaw,
            ref = ref,
        )
        check(Cosign.hexEqual(derivedParams, paramsHash)) {
            "arca: venue hop paramsHash mismatch: server returned $paramsHash, the returned parameters hash to " +
                "$derivedParams — do not sign this proposal"
        }

        val derivedDigest = Cosign.operatorActionDigest(
            chainId = d.chainId,
            kernelAddress = d.verifyingContract,
            action = action,
            boundary = boundaryKey,
            paramsHash = derivedParams,
            nonce = nonce,
            deadline = deadline,
        )
        check(Cosign.hexEqual(derivedDigest, digest)) {
            "arca: venue hop digest mismatch: server returned $digest, the returned parameters digest to " +
                "$derivedDigest — do not sign this proposal"
        }
    }
}

/** The accepted co-signed hop. */
@Serializable
public data class VenueHopResponse(
    override val operation: Operation,
    /** The debited boundary. */
    public val boundaryId: String = "",
    /** The credited boundary. */
    public val targetBoundaryId: String = "",
) : OperationResponse {
    override fun withOperation(operation: Operation): VenueHopResponse = copy(operation = operation)
}

/**
 * Move capital straight from one exchange object to another.
 *
 * A transfer whose source and target are both exchange objects hops
 * venue-to-venue in a single on-chain frame — no intermediate denominated
 * arca, and the value never rests at a boundary. Hops carry no transfer fee
 * and work across isolation boundaries.
 *
 * This is [transfer] plus the co-sign fallback. On an unarmed source boundary
 * it is exactly a transfer. On an armed one the plain call is refused (the
 * kernel will not move value out without the owner's signature), so this
 * proposes the hop, hands the digest to [sign], and submits.
 *
 * ```kotlin
 * val res = arca.hopVenues(
 *     path = "/op/transfer/rebalance-1",
 *     from = "/users/alice/exchange/hl",
 *     to = "/users/alice/exchange/paper",
 *     amount = "500",
 *     sign = { digest, _ -> wallet.sign(digest) },
 * ).settle()
 * ```
 *
 * Destinations are limited to Hyperliquid and the paper venue; a GLL-paper
 * target is refused, because its account is credited by faucet rather than by
 * value landing at the venue contract.
 *
 * @param sign Called ONLY if the source boundary is co-sign armed. Leave it
 *   null on unarmed boundaries — it is never invoked there. Leaving it null on
 *   an ARMED boundary throws [ArcaException.CosignRequired] unchanged.
 * @param deadline Optional unix-seconds co-signature expiry, used only on the
 *   signed path.
 */
public fun Arca.hopVenues(
    path: String,
    from: String,
    to: String,
    amount: String,
    sign: CosignDigestSigner? = null,
    deadline: Long = 0,
): OperationHandle<VenueHopResponse> =
    operationHandle {
        try {
            val body = arcaJson.encodeToJsonElement(
                PlainTransferRequest.serializer(),
                PlainTransferRequest(
                    realmId = realm,
                    path = path,
                    sourceArcaPath = from,
                    targetArcaPath = to,
                    amount = amount,
                ),
            )
            val plain: TransferResponse = client.post("/transfer", body = body)
            VenueHopResponse(operation = plain.operation)
        } catch (e: ArcaException.CosignRequired) {
            // Only the armed-boundary refusal is recoverable here, and only
            // with a signer. Every other refusal — an unhoppable destination,
            // a fee, an insufficient balance — propagates untouched, because
            // retrying it under a signature would fail again after asking the
            // key holder for one.
            if (sign == null) throw e
            val proposal = proposeVenueHop(path = path, from = from, to = to, amount = amount, deadline = deadline)
            // Re-derive before handing the key a hash it cannot read. A server
            // that returned a digest for a different destination or a larger
            // amount is caught here rather than on-chain.
            proposal.verify()
            val signature = sign(proposal.digest, proposal)
            submitVenueHopRequest(
                path = path,
                from = from,
                to = to,
                amount = amount,
                nonce = proposal.nonce,
                deadline = proposal.deadline,
                signature = signature,
                ref = proposal.ref,
            )
        }
    }

/**
 * Whether one co-signature nonce can still be spent on a boundary.
 *
 * Returned by [getCosignNonceState]. Read [spendable] — it is the only field
 * correct on both kernel generations.
 */
@Serializable
public data class CosignNonceState(
    public val boundaryId: String = "",
    /** The nonce that was checked, echoed as a decimal string. */
    public val nonce: String = "",
    /**
     * The single answer most callers want: can this envelope still be
     * submitted? Correct on both kernel generations.
     */
    public val spendable: Boolean = false,
    /**
     * Burn-set read: `true` means this slot is spent.
     *
     * Meaningful **only when [unordered] is true**. A frozen-counter kernel
     * has no burn set, so this is always `false` there — including for nonces
     * the counter will refuse. Prefer [spendable].
     */
    public val consumed: Boolean = false,
    /**
     * `true` on a burn-set kernel (marker 7+) that accepts caller-chosen
     * nonces; `false` on a frozen-counter kernel (marker 3-6), where
     * [counterNonce] is the only value it will accept.
     */
    public val unordered: Boolean = false,
    /**
     * The boundary's live counter, present only when [unordered] is false. On
     * such a kernel an envelope is live iff it was signed over exactly this.
     */
    public val counterNonce: String? = null,
    /**
     * Why an unspendable slot is gone: [COSIGN_NONCE_EXECUTED],
     * [COSIGN_NONCE_REVOKED], or [COSIGN_NONCE_UNKNOWN].
     *
     * `null` when [spendable] is true — an unburned slot has no burn to
     * explain. Executed and revoked are opposite answers to "did the value
     * move?", so do not collapse them, and do not read unknown as either one:
     * only revoked licenses asserting that nothing moved.
     */
    public val disposition: String? = null,
    /** The transaction that burned the slot, when one was found. */
    public val txHash: String? = null,
    /**
     * The platform operation the burn belongs to. Present only for an executed
     * burn the platform submitted — a revocation is the owner acting directly
     * on the kernel, so it has no operation.
     */
    public val operationId: String? = null,
)

/**
 * Check whether a co-signature's nonce can still be spent.
 *
 * Use this before submitting an envelope that has been outstanding long enough
 * to have been overtaken — a retry that raced the original, a second device,
 * or a user who cancelled the approval. Submitting a spent nonce throws
 * [ArcaException.CosignNonceUsed]; this read tells you first, so you can
 * re-propose without asking for a signature that cannot land.
 *
 * ```kotlin
 * val state = arca.getCosignNonceState("bnd_abc", proposal.nonce)
 * if (!state.spendable) {
 *     // re-propose rather than signing a dead slot
 * }
 * ```
 *
 * Read [CosignNonceState.spendable], not `consumed`: on a frozen-counter
 * kernel (marker 3-6) there is no burn set, so `consumed` is always `false`
 * even for a nonce the kernel will refuse.
 *
 * This answers about the nonce, not the signature over it. A spendable nonce
 * means submitting is not futile — not that the envelope will verify.
 */
public suspend fun Arca.getCosignNonceState(boundaryId: String, nonce: String): CosignNonceState =
    client.get(
        "/custody/boundaries/$boundaryId/cosign-nonces/$nonce",
        query = mapOf("realmId" to realm),
    )

/**
 * Signable fields for a co-signed venue hop. Nothing is persisted and no funds
 * move.
 *
 * Only the SOURCE boundary signs — value arriving is consent-free, so the
 * destination's owner never has to be online.
 *
 * [path] is required because the signed ref is derived from it: a submit at a
 * different path produces a different digest and is refused. A careful signer
 * should re-derive [VenueHopProposal.digest] from the returned fields
 * (encoding `amountRaw`, never `amount`) and refuse on mismatch rather than
 * blind-signing the server's hash.
 *
 * Most callers want [hopVenues], which does propose → sign → submit and skips
 * all of it on an unarmed boundary.
 */
public suspend fun Arca.proposeVenueHop(
    path: String,
    from: String,
    to: String,
    amount: String,
    deadline: Long = 0,
): VenueHopProposal {
    val body = arcaJson.encodeToJsonElement(
        VenueHopProposeRequest.serializer(),
        VenueHopProposeRequest(
            path = path,
            sourceArcaPath = from,
            targetArcaPath = to,
            amount = amount,
            deadline = deadline,
        ),
    )
    return client.post(
        "/custody/venue-hops/propose",
        query = mapOf("realmId" to realm),
        body = body,
    )
}

/**
 * Submit a venue hop co-signed by the source boundary's wallet.
 *
 * The server re-derives the digest and verifies the signature against that
 * boundary's on-chain co-sign key before anything moves. [path], [amount],
 * [nonce], and [deadline] must match what was signed.
 *
 * @param ref Optional. The server derives the authoritative ref from [path];
 *   supplying one only cross-checks it, so a ref from a different path is
 *   refused by name rather than surfacing as an opaque signature mismatch.
 */
public fun Arca.submitVenueHop(
    path: String,
    from: String,
    to: String,
    amount: String,
    nonce: String,
    deadline: Long,
    signature: String,
    ref: String? = null,
): OperationHandle<VenueHopResponse> =
    operationHandle {
        submitVenueHopRequest(path, from, to, amount, nonce, deadline, signature, ref)
    }

/**
 * The bare HTTP call, shared by [submitVenueHop] and the fallback inside
 * [hopVenues] — which already runs inside an operation handle and must not
 * start a second one.
 */
private suspend fun Arca.submitVenueHopRequest(
    path: String,
    from: String,
    to: String,
    amount: String,
    nonce: String,
    deadline: Long,
    signature: String,
    ref: String?,
): VenueHopResponse {
    val body = arcaJson.encodeToJsonElement(
        VenueHopSubmitRequest.serializer(),
        VenueHopSubmitRequest(
            path = path,
            sourceArcaPath = from,
            targetArcaPath = to,
            amount = amount,
            nonce = nonce,
            deadline = deadline,
            signature = signature,
            ref = ref,
        ),
    )
    return client.post(
        "/custody/venue-hops",
        query = mapOf("realmId" to realm),
        body = body,
    )
}

@Serializable
private data class PlainTransferRequest(
    val realmId: String,
    val path: String,
    val sourceArcaPath: String,
    val targetArcaPath: String,
    val amount: String,
)

@Serializable
private data class VenueHopProposeRequest(
    val path: String,
    val sourceArcaPath: String,
    val targetArcaPath: String,
    val amount: String,
    val deadline: Long,
)

@Serializable
private data class VenueHopSubmitRequest(
    val path: String,
    val sourceArcaPath: String,
    val targetArcaPath: String,
    val amount: String,
    val nonce: String,
    val deadline: Long,
    val signature: String,
    // Omitted when null (explicitNulls = false), which is what lets the server
    // derive the authoritative ref instead of cross-checking an empty one.
    val ref: String? = null,
)
