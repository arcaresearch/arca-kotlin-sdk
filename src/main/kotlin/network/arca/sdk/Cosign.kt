package network.arca.sdk

import network.arca.sdk.internal.Keccak
import java.math.BigInteger

/**
 * The EIP-712 domain the v7 kernel line verifies co-signatures against.
 *
 * Only `chainId` and `verifyingContract` vary between realms. A proposal
 * claiming a different name or version describes a kernel this SDK does not
 * know how to hash for, and is refused rather than signed.
 */
public const val COSIGN_DOMAIN_NAME: String = "ArcaCustodyKernel"
public const val COSIGN_DOMAIN_VERSION: String = "2"

/**
 * The discriminator bound into the EIP-712 digest. It is what stops a
 * signature collected for one action from authorizing another.
 */
public object CosignAction {
    public const val TRANSFER_BETWEEN_VENUES: Int = 11
    public const val TRANSFER_BETWEEN_VENUES_PRE_AUTH: Int = 12
}

/**
 * Derives the hashes a co-signature commits to.
 *
 * A co-signature over a digest you did not compute is an attestation to a
 * 32-byte number you cannot read. These helpers let a signer re-derive that
 * number from the semantic fields it was shown — destination venue, amount,
 * boundary — and refuse when the two disagree.
 *
 * Signing itself is deliberately absent. It needs secp256k1, which this SDK
 * does not depend on and should not hand-roll; the key belongs in a keystore,
 * an HSM, or the user's device anyway. Verify here, sign there.
 */
public object Cosign {
    private val EIP712_DOMAIN_TYPEHASH: ByteArray = Keccak.digest(
        "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)".toByteArray(),
    )
    private val OPERATOR_ACTION_TYPEHASH: ByteArray = Keccak.digest(
        "OperatorAction(uint8 actionType,bytes32 boundary,bytes32 paramsHash,uint256 nonce,uint256 deadline)"
            .toByteArray(),
    )

    /** The EIP-712 struct typehash, for cross-checking against the kernel's constant. */
    public val operatorActionTypehash: String get() = "0x" + OPERATOR_ACTION_TYPEHASH.toHex()

    /**
     * The EIP-712 domain separator for a kernel — equal to its
     * `eip712DomainSeparator()` view, derived locally so a signer never has
     * to trust a server's answer for it.
     */
    public fun domainSeparator(chainId: Long, kernelAddress: String): String {
        val buf = EIP712_DOMAIN_TYPEHASH +
            Keccak.digest(COSIGN_DOMAIN_NAME.toByteArray()) +
            Keccak.digest(COSIGN_DOMAIN_VERSION.toByteArray()) +
            abiUint(BigInteger.valueOf(chainId)) +
            abiAddress(kernelAddress, "verifyingContract")
        return "0x" + Keccak.digest(buf).toHex()
    }

    /**
     * The digest a co-sign key must sign, equal to the kernel's
     * `hashOperatorAction(...)` view.
     *
     * [nonce] is decimal. On the v7 line nonces are unordered: any unused
     * value works and each is single-use.
     */
    public fun operatorActionDigest(
        chainId: Long,
        kernelAddress: String,
        action: Int,
        boundary: String,
        paramsHash: String,
        nonce: String,
        deadline: Long,
    ): String {
        val structHash = Keccak.digest(
            OPERATOR_ACTION_TYPEHASH +
                abiUint(BigInteger.valueOf(action.toLong())) +
                abiBytes32(boundary, "boundary") +
                abiBytes32(paramsHash, "paramsHash") +
                abiUint(abiDecimal(nonce, "nonce")) +
                abiUint(BigInteger.valueOf(deadline)),
        )
        val separator = abiBytes32(domainSeparator(chainId, kernelAddress), "domainSeparator")
        return "0x" + Keccak.digest(byteArrayOf(0x19, 0x01) + separator + structHash).toHex()
    }

    /**
     * The `paramsHash` of a venue-to-venue hop.
     *
     * Matches the kernel's `keccak256(abi.encode(...))` over the same fields.
     * The exact (action 11) and capped pre-auth (action 12) forms share this
     * preimage; only the action discriminator in the digest separates them.
     *
     * [amount] is the uint256 the kernel moves, in token base units — a
     * proposal's `amountRaw`, never its human-decimal `amount`. Encoding the
     * decimal produces a different hash and a signature the kernel rejects.
     */
    @Suppress("LongParameterList")
    public fun transferBetweenVenuesParamsHash(
        fromVenue: String,
        fromBoundary: String,
        toBoundary: String,
        toVenue: String,
        toVenueAccountId: String,
        token: String,
        amount: String,
        ref: String,
    ): String {
        val buf = abiAddress(fromVenue, "fromVenue") +
            abiBytes32(fromBoundary, "fromBoundary") +
            abiBytes32(toBoundary, "toBoundary") +
            abiAddress(toVenue, "toVenue") +
            abiBytes32(toVenueAccountId, "toVenueAccountId") +
            abiAddress(token, "token") +
            abiUint(abiDecimal(amount, "amount")) +
            abiBytes32(ref, "ref")
        return "0x" + Keccak.digest(buf).toHex()
    }

    // --- ABI word encoding ---

    private fun abiUint(v: BigInteger): ByteArray {
        require(v.signum() >= 0) { "arca: uint256 cannot be negative" }
        require(v.bitLength() <= 256) { "arca: value overflows uint256" }
        val out = ByteArray(32)
        val raw = v.toByteArray() // may carry a leading sign byte
        val src = if (raw.size > 32) raw.copyOfRange(raw.size - 32, raw.size) else raw
        src.copyInto(out, 32 - src.size)
        return out
    }

    private fun abiDecimal(s: String, field: String): BigInteger =
        try {
            BigInteger(s.trim())
        } catch (e: NumberFormatException) {
            throw IllegalArgumentException("arca: $field is not a decimal integer: \"$s\"", e)
        }

    private fun abiAddress(s: String, field: String): ByteArray {
        val raw = decodeHex(s, field)
        require(raw.size == 20) { "arca: $field must be a 20-byte address, got ${raw.size} bytes" }
        val out = ByteArray(32)
        raw.copyInto(out, 12)
        return out
    }

    private fun abiBytes32(s: String, field: String): ByteArray {
        val raw = decodeHex(s, field)
        require(raw.size == 32) { "arca: $field must be a 32-byte word, got ${raw.size} bytes" }
        return raw
    }

    private fun decodeHex(s: String, field: String): ByteArray {
        val body = s.trim().removePrefix("0x").removePrefix("0X")
        require(body.isNotEmpty()) { "arca: $field is empty" }
        require(body.length % 2 == 0) { "arca: $field has an odd hex length" }
        return ByteArray(body.length / 2) { i ->
            val hi = Character.digit(body[i * 2], 16)
            val lo = Character.digit(body[i * 2 + 1], 16)
            require(hi >= 0 && lo >= 0) { "arca: $field is not valid hex: \"$s\"" }
            ((hi shl 4) or lo).toByte()
        }
    }

    private fun ByteArray.toHex(): String {
        val digits = "0123456789abcdef"
        val out = StringBuilder(size * 2)
        for (b in this) {
            val v = b.toInt() and 0xFF
            out.append(digits[v ushr 4]).append(digits[v and 0x0F])
        }
        return out.toString()
    }

    internal fun hexEqual(a: String, b: String): Boolean =
        a.removePrefix("0x").removePrefix("0X").equals(b.removePrefix("0x").removePrefix("0X"), ignoreCase = true)
}
