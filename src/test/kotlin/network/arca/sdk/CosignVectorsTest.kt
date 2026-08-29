package network.arca.sdk

import network.arca.sdk.internal.Keccak
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * Golden vectors from `sdk/typescript/src/fixtures/cosign-vectors.json`, the
 * cross-SDK contract for the co-signed OperatorAction wire format. That
 * fixture is pinned against the authoritative kernel by
 * `backend/contracts/test/v7/CosignVectors.t.sol`, so agreeing with it here is
 * agreeing with the chain.
 *
 * A failure means one of two very different things. If the fixture was
 * regenerated, these constants are stale — update them. If it was not, this
 * SDK derives a digest the kernel would reject, and every signature it
 * verifies is worthless.
 */
internal object Vectors {
    const val CHAIN_ID = 998L
    const val KERNEL = "0x1111111111111111111111111111111111111111"
    const val DOMAIN_SEPARATOR = "0xce2ffc6a26f978eacc195d1d9872aff6655c30e93d4a50ae143c2c7332a92d77"
    const val TYPEHASH = "0x72c47eb99437aa8c5d7633e14eb25453c9518b3cb628a9db653ca36935b792ea"

    const val FROM_VENUE = "0x000000000000000000000000000000000000beef"
    const val BOUNDARY = "0x00000000000000000000000000000000000000000000000000000000000000b0"
    const val TO_BOUNDARY = "0x00000000000000000000000000000000000000000000000000000000000000b1"
    const val TO_VENUE = "0x000000000000000000000000000000000000feed"
    const val TO_VENUE_ACCOUNT = "0x0000000000000000000000000000000000000000000000000000000000000009"
    const val TOKEN = "0xb88339CB7199b77E23DB6E890353E22632Ba630f"
    const val AMOUNT_RAW = "75000000"
    const val REF = "0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
    const val PARAMS_HASH = "0x760217cbdcb92a92a352c5d3253ea65800224dc1ba88ed2cf4aeafe54a735037"

    const val NONCE = "12"
    const val DEADLINE = 1900000000L
    const val DIGEST = "0x684ecc5abcf75bf9258a26a74095027b1be24568ae4d9dd5faa545382ae68581"

    const val PRE_AUTH_NONCE = "13"
    const val PRE_AUTH_DIGEST = "0x70be367f6c3880a63b8e89d845af1899f4f71e7050886c8f7b8caad1025b49cc"

    fun proposal(): VenueHopProposal = VenueHopProposal(
        action = CosignAction.TRANSFER_BETWEEN_VENUES,
        boundaryId = "bnd_src",
        boundaryKey = BOUNDARY,
        targetBoundaryId = "bnd_dst",
        targetBoundaryKey = TO_BOUNDARY,
        fromVenue = FROM_VENUE,
        toVenue = TO_VENUE,
        toVenueAccountKey = TO_VENUE_ACCOUNT,
        token = TOKEN,
        domain = CosignDomain(
            name = COSIGN_DOMAIN_NAME,
            version = COSIGN_DOMAIN_VERSION,
            chainId = CHAIN_ID,
            verifyingContract = KERNEL,
        ),
        amount = "75",
        amountRaw = AMOUNT_RAW,
        ref = REF,
        nonce = NONCE,
        deadline = DEADLINE,
        paramsHash = PARAMS_HASH,
        digest = DIGEST,
    )

    fun hopParamsHash(): String = Cosign.transferBetweenVenuesParamsHash(
        fromVenue = FROM_VENUE,
        fromBoundary = BOUNDARY,
        toBoundary = TO_BOUNDARY,
        toVenue = TO_VENUE,
        toVenueAccountId = TO_VENUE_ACCOUNT,
        token = TOKEN,
        amount = AMOUNT_RAW,
        ref = REF,
    )
}

private fun ByteArray.hex(): String = joinToString("") { "%02x".format(it) }

class KeccakTest {
    /**
     * The published Keccak-256 vectors. These are what separate a correct
     * permutation from one that merely runs — and specifically what catches
     * the SHA-3 padding confusion, since SHA3-256("") is a completely
     * different digest.
     */
    @Test
    fun `matches the published Keccak-256 vectors`() {
        assertEquals(
            "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
            Keccak.digest(ByteArray(0)).hex(),
            "Keccak-256 of the empty string",
        )
        assertEquals(
            "4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45",
            Keccak.digest("abc".toByteArray()).hex(),
        )
        assertEquals(
            "1c8aff950685c2ed4bc3174f3472287b56d9517b9c948127319a09a7a36deac8",
            Keccak.digest("hello".toByteArray()).hex(),
        )
    }

    /**
     * The absorb loop and the padding branch are different code paths, and a
     * block-boundary bug hides from short inputs. The rate is 136 bytes, so
     * these straddle it in both directions and land exactly on it.
     */
    /**
     * A 136-byte input is the one that fills the rate exactly, so its padding
     * needs a whole extra block. Getting that wrong is invisible to short
     * inputs and to the co-sign vectors, whose preimages are all one block.
     *
     * Pinned against an independent implementation (Go's
     * `sha3.NewLegacyKeccak256`) over `0,1,2,…` truncated to a byte.
     */
    @Test
    fun `pads correctly across the 136-byte rate boundary`() {
        val expected = mapOf(
            135 to "cbdfd9dee5faad3818d6b06f95a219fd290b0e1706f6a82e5a595b9ce9faca62",
            136 to "7ce759f1ab7f9ce437719970c26b0a66ff11fe3e38e17df89cf5d29c7d7f807e",
            137 to "ac73d4fae68b8453f764007c1a20ce95994187861f0c3227a3a8e99a73a3b1db",
        )
        for ((length, want) in expected) {
            val input = ByteArray(length) { (it % 251).toByte() }
            assertEquals(want, Keccak.digest(input).hex(), "Keccak-256 of a $length-byte input")
        }
    }
}

class CosignVectorsTest {
    @Test
    fun `operator action typehash matches the golden vector`() {
        assertTrue(Cosign.hexEqual(Cosign.operatorActionTypehash, Vectors.TYPEHASH))
    }

    @Test
    fun `domain separator matches the golden vector`() {
        assertTrue(
            Cosign.hexEqual(Cosign.domainSeparator(Vectors.CHAIN_ID, Vectors.KERNEL), Vectors.DOMAIN_SEPARATOR),
            "domain separator disagrees with the kernel",
        )
    }

    @Test
    fun `venue hop paramsHash matches the golden vector`() {
        assertTrue(Cosign.hexEqual(Vectors.hopParamsHash(), Vectors.PARAMS_HASH))
    }

    /**
     * The exact and capped forms share one preimage; only the action
     * discriminator inside the digest separates them. If that stopped being
     * true, a signature authorizing a ceiling would also authorize an exact
     * move.
     */
    @Test
    fun `exact and pre-auth share a paramsHash but not a digest`() {
        val paramsHash = Vectors.hopParamsHash()
        val exact = Cosign.operatorActionDigest(
            chainId = Vectors.CHAIN_ID,
            kernelAddress = Vectors.KERNEL,
            action = CosignAction.TRANSFER_BETWEEN_VENUES,
            boundary = Vectors.BOUNDARY,
            paramsHash = paramsHash,
            nonce = Vectors.NONCE,
            deadline = Vectors.DEADLINE,
        )
        val preAuth = Cosign.operatorActionDigest(
            chainId = Vectors.CHAIN_ID,
            kernelAddress = Vectors.KERNEL,
            action = CosignAction.TRANSFER_BETWEEN_VENUES_PRE_AUTH,
            boundary = Vectors.BOUNDARY,
            paramsHash = paramsHash,
            nonce = Vectors.PRE_AUTH_NONCE,
            deadline = Vectors.DEADLINE,
        )
        assertTrue(Cosign.hexEqual(exact, Vectors.DIGEST), "exact digest disagrees with the kernel")
        assertTrue(Cosign.hexEqual(preAuth, Vectors.PRE_AUTH_DIGEST), "pre-auth digest disagrees with the kernel")
        assertNotEquals(exact, preAuth, "the action discriminator is not bound into the digest")
    }

    @Test
    fun `verify accepts the golden proposal`() {
        Vectors.proposal().verify()
    }

    /**
     * Every field of the preimage must move the paramsHash. A field that does
     * not is a field an attacker can change after the signature is collected.
     */
    @Test
    fun `every hop field is bound into the paramsHash`() {
        val base = Vectors.hopParamsHash()
        val mutations: Map<String, VenueHopProposal> = mapOf(
            "fromVenue" to Vectors.proposal().copy(fromVenue = "0x000000000000000000000000000000000000bee0"),
            "fromBoundary" to Vectors.proposal().copy(boundaryKey = Vectors.TO_BOUNDARY),
            "toBoundary" to Vectors.proposal().copy(targetBoundaryKey = Vectors.BOUNDARY),
            "toVenue" to Vectors.proposal().copy(toVenue = "0x000000000000000000000000000000000000fee0"),
            "toVenueAccountId" to Vectors.proposal().copy(toVenueAccountKey = Vectors.REF),
            "token" to Vectors.proposal().copy(token = Vectors.KERNEL),
            "amount" to Vectors.proposal().copy(amountRaw = "75000001"),
            "ref" to Vectors.proposal().copy(ref = Vectors.TO_VENUE_ACCOUNT),
        )
        for ((field, p) in mutations) {
            val mutated = Cosign.transferBetweenVenuesParamsHash(
                fromVenue = p.fromVenue,
                fromBoundary = p.boundaryKey,
                toBoundary = p.targetBoundaryKey,
                toVenue = p.toVenue,
                toVenueAccountId = p.toVenueAccountKey,
                token = p.token,
                amount = p.amountRaw,
                ref = p.ref,
            )
            assertNotEquals(base, mutated, "changing $field left the paramsHash unchanged; it is not bound")
        }
    }

    @Test
    fun `verify refuses a domain it cannot derive`() {
        val p = Vectors.proposal()
        assertThrows(IllegalStateException::class.java) {
            p.copy(domain = p.domain!!.copy(version = "3")).verify()
        }
    }

    @Test
    fun `verify catches a tampered digest`() {
        assertThrows(IllegalStateException::class.java) {
            Vectors.proposal().copy(digest = Vectors.PRE_AUTH_DIGEST).verify()
        }
    }

    /**
     * The attack `verify` exists to catch: the paramsHash and digest are the
     * server's originals, and only the amount the caller is shown has moved.
     */
    @Test
    fun `verify catches a raised amount`() {
        assertThrows(IllegalStateException::class.java) {
            Vectors.proposal().copy(amountRaw = "750000000").verify()
        }
    }
}
