package network.arca.sdk

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonObject
import network.arca.sdk.internal.arcaJson
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Base64

/**
 * Venue-hop tests.
 *
 * `hopVenues` exists so a caller does not have to know whether the source
 * boundary is co-sign armed: it runs the plain transfer, and only if that is
 * refused does it propose, sign, and submit. These pin both arms and, more
 * importantly, the refusals that must NOT take the signed path.
 */
class ArcaVenueHopTest {

    private lateinit var server: MockWebServer
    private val paths = mutableListOf<String>()
    private val bodies = mutableListOf<JsonObject>()

    /** When true, the plain transfer answers 412 like an armed boundary. */
    private var armed = false

    /** When set, the plain transfer answers with this refusal instead. */
    private var plainRefusal: Pair<String, String>? = null

    /** When true, propose returns a destination its own paramsHash disowns. */
    private var tampered = false

    /** When true, the submit answers 412 COSIGN_NONCE_USED. */
    private var nonceUsedOnSubmit = false

    /** When true, the nonce-state read answers as a pre-v7 counter kernel. */
    private var counterKernel = false

    /** GET paths, kept separately so the POST-ordering assertions stay exact. */
    private val getPaths = mutableListOf<String>()

    @BeforeEach
    fun setUp() {
        paths.clear()
        bodies.clear()
        getPaths.clear()
        armed = false
        plainRefusal = null
        tampered = false
        nonceUsedOnSubmit = false
        counterKernel = false
        server = MockWebServer()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val path = request.path ?: ""
                if (request.method == "POST") {
                    paths.add(path)
                    val raw = request.body.readUtf8()
                    if (raw.isNotEmpty()) bodies.add(arcaJson.parseToJsonElement(raw).jsonObject)
                } else {
                    getPaths.add(path)
                }
                return when {
                    path.contains("/cosign-nonces/") ->
                        json(if (counterKernel) NONCE_STATE_COUNTER else NONCE_STATE_CONSUMED)
                    path.contains("/custody/venue-hops/propose") ->
                        json(if (tampered) TAMPERED_PROPOSAL else PROPOSAL)
                    path.contains("/custody/venue-hops") ->
                        if (nonceUsedOnSubmit) json(NONCE_USED_REFUSAL, code = 412) else json(SUBMITTED)
                    path.contains("/transfer") -> when {
                        armed -> json(COSIGN_REFUSAL, code = 412)
                        plainRefusal != null -> json(
                            """{"success":false,"error":{"code":"${plainRefusal!!.first}","message":"${plainRefusal!!.second}"}}""",
                            code = 400,
                        )
                        else -> json(PLAIN_TRANSFER)
                    }
                    else -> json("""{"success":true,"data":{}}""")
                }
            }
        }
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    @Test
    fun `unarmed boundary is a plain transfer and never signs`() = runBlocking {
        var signed = false
        val res = makeArca().hopVenues(
            path = "/op/transfer/hop-1",
            from = "/users/a/exchange/hl",
            to = "/users/b/exchange/paper",
            amount = "500",
            sign = { _, _ -> signed = true; "0xsig" },
        ).submitted()

        assertEquals("op_plain", res.operation.id.toString())
        assertFalse(signed, "the signer ran on an unarmed boundary; no signature is needed there")
        assertEquals(1, paths.size, "issued more than the plain transfer: $paths")
    }

    @Test
    fun `armed boundary proposes, signs, and submits`() = runBlocking {
        armed = true
        var sawDigest = ""
        var sawAmountRaw = ""
        val res = makeArca().hopVenues(
            path = "/op/transfer/hop-2",
            from = "/users/a/exchange/hl",
            to = "/users/b/exchange/paper",
            amount = "500",
            sign = { digest, proposal ->
                sawDigest = digest
                sawAmountRaw = proposal.amountRaw
                "0xsignature"
            },
        ).submitted()

        assertEquals(Vectors.DIGEST, sawDigest, "the signer must see the kernel-derived digest")
        // The paramsHash commits to the raw uint256; a signer that encodes the
        // decimal string produces a hash the kernel will not match.
        assertEquals(Vectors.AMOUNT_RAW, sawAmountRaw)
        assertEquals("bnd_src", res.boundaryId)
        assertEquals("bnd_dst", res.targetBoundaryId)
        assertEquals(3, paths.size, "want transfer + propose + submit, got $paths")

        // These routes read the realm from the query string, and the ordinary
        // post() sends none — so both must go through the query overload.
        assertTrue(paths[1].contains("realmId="), "propose carried no realmId: ${paths[1]}")
        assertTrue(paths[2].contains("realmId="), "submit carried no realmId: ${paths[2]}")

        val propose = bodies[1]
        val submit = bodies[2]
        assertEquals("/op/transfer/hop-2", propose["path"]?.jsonPrimitive?.content)
        // The signed ref derives from the operation path, so submitting at a
        // different path than was proposed would be refused server-side.
        assertEquals(
            propose["path"]?.jsonPrimitive?.content,
            submit["path"]?.jsonPrimitive?.content,
        )
        assertEquals("0xsignature", submit["signature"]?.jsonPrimitive?.content)
        assertEquals(Vectors.NONCE, submit["nonce"]?.jsonPrimitive?.content)
    }

    /**
     * The reason the digest is re-derived rather than relayed. A server that
     * describes one hop and asks for a signature over another is caught before
     * the key is used, so the co-signature means what the user was shown.
     */
    @Test
    fun `a proposal whose digest does not match its parameters never reaches the signer`() {
        armed = true
        tampered = true
        var signed = false

        assertThrows(IllegalStateException::class.java) {
            runBlocking {
                makeArca().hopVenues(
                    path = "/op/transfer/hop-3",
                    from = "/users/a/exchange/hl",
                    to = "/users/b/exchange/paper",
                    amount = "500",
                    sign = { _, _ -> signed = true; "0xsignature" },
                ).submitted()
            }
        }

        assertFalse(signed, "the signer was handed a digest the proposal's own fields do not produce")
        assertEquals(2, paths.size, "want transfer + propose and no submit, got $paths")
    }

    @Test
    fun `armed boundary without a signer surfaces the typed challenge`() {
        armed = true
        val err = assertThrows(ArcaException.CosignRequired::class.java) {
            runBlocking {
                makeArca().hopVenues(
                    path = "/op/transfer/hop-3",
                    from = "/a/exchange",
                    to = "/b/exchange",
                    amount = "5",
                ).submitted()
            }
        }
        assertEquals("transfer.venue_hop", err.challenge.surface)
        assertEquals("bnd_src", err.challenge.boundaryId)
        assertEquals("/api/v1/custody/venue-hops/propose", err.challenge.propose)
        assertEquals(1, paths.size, "issued more than the refused transfer: $paths")
    }

    @Test
    fun `a refusal a signature cannot fix does not take the signed path`() {
        plainRefusal = "VALIDATION_ERROR" to "Venue-to-venue transfers cannot charge a transfer fee"
        var signed = false
        assertThrows(ArcaException.Validation::class.java) {
            runBlocking {
                makeArca().hopVenues(
                    path = "/op/transfer/hop-4",
                    from = "/a/exchange",
                    to = "/b/exchange",
                    amount = "5",
                    sign = { _, _ -> signed = true; "0xsig" },
                ).submitted()
            }
        }
        assertFalse(signed, "the signer ran for a refusal a signature cannot fix")
    }

    /**
     * A spent nonce is an ordinary lifecycle outcome — a retry racing the
     * original, or a user who cancelled — not a signing failure. Before the
     * dedicated code existed, the only signal separating the two was message
     * text, so integrators reported "that approval didn't match this request"
     * for signatures that were perfectly valid.
     */
    @Test
    fun `a spent nonce surfaces as its own typed error, not a validation failure`() {
        plainRefusal = null
        armed = true
        // Propose succeeds; the submit is what finds the slot gone.
        nonceUsedOnSubmit = true

        val err = assertThrows(ArcaException.CosignNonceUsed::class.java) {
            runBlocking {
                makeArca().hopVenues(
                    path = "/op/transfer/hop-6",
                    from = "/a/exchange",
                    to = "/b/exchange",
                    amount = "5",
                    sign = { _, _ -> "0xsignature" },
                ).submitted()
            }
        }
        assertEquals("bnd_src", err.details.boundaryId)
        assertEquals("nonce_consumed", err.details.reason)
        assertEquals(Vectors.NONCE, err.details.nonce)
        assertTrue(
            err.details.resolution?.contains("re-propose") == true,
            "the refusal must name the remedy: ${err.details.resolution}",
        )
    }

    @Test
    fun `getCosignNonceState reads the burn set on an unordered kernel`() = runBlocking {
        val state = makeArca().getCosignNonceState("bnd_v7", "9223372036854775807")

        assertFalse(state.spendable)
        assertTrue(state.consumed)
        assertTrue(state.unordered)
        // A 63-bit nonce exceeds what a JSON number carries losslessly in
        // every consumer, so it must round-trip as a string.
        assertEquals("9223372036854775807", state.nonce)
        assertNull(state.counterNonce, "surfacing the frozen slot invites signing against it")
        assertTrue(getPaths[0].contains("realmId="), "the read carried no realmId: ${getPaths[0]}")
        assertTrue(getPaths[0].contains("/cosign-nonces/9223372036854775807"))
    }

    /**
     * The trap: a frozen-counter kernel has no burn set, so `consumed` is
     * structurally false even for a nonce it will refuse. Callers must be able
     * to trust `spendable` alone.
     */
    @Test
    fun `getCosignNonceState reports not-spendable on a counter kernel despite consumed false`() = runBlocking {
        counterKernel = true
        val state = makeArca().getCosignNonceState("bnd_k5", "8")

        assertFalse(state.consumed, "consumed is structural on a kernel with no burn set")
        assertFalse(state.spendable)
        assertFalse(state.unordered)
        assertEquals("9", state.counterNonce)
    }

    @Test
    fun `submitVenueHop omits an unset ref so the server derives it`() = runBlocking {
        makeArca().submitVenueHop(
            path = "/op/transfer/hop-5",
            from = "/a/exchange",
            to = "/b/exchange",
            amount = "25",
            nonce = "3",
            deadline = 1893456000,
            signature = "0xsig",
        ).submitted()

        val body = bodies[0]
        assertEquals("0xsig", body["signature"]?.jsonPrimitive?.content)
        // A supplied ref is a cross-check; sending an empty one would fail it.
        assertNull(body["ref"], "an unset ref was serialized")
        assertTrue(paths[0].contains("realmId="))
    }

    // ---- helpers ----

    private fun json(body: String, code: Int = 200): MockResponse =
        MockResponse().setResponseCode(code).setHeader("Content-Type", "application/json").setBody(body)

    private fun makeArca(): Arca =
        Arca(token = fakeJwt(), baseUrl = server.url("/").toString().trimEnd('/'))

    private fun fakeJwt(): String {
        val enc = Base64.getUrlEncoder().withoutPadding()
        val header = enc.encodeToString("""{"alg":"HS256","typ":"JWT"}""".toByteArray())
        val payload = enc.encodeToString("""{"realmId":"rlm_test","sub":"usr_test"}""".toByteArray())
        return "$header.$payload.fakesig"
    }

    private companion object {
        /** Operation requires realmId/path/type/createdAt/updatedAt to decode. */
        fun operation(id: String): String = """{"id":"$id","realmId":"rlm_test",
            "path":"/op/transfer/hop","type":"transfer","state":"completed",
            "createdAt":"2026-08-29T00:00:00Z","updatedAt":"2026-08-29T00:00:00Z"}"""

        val PLAIN_TRANSFER =
            """{"success":true,"data":{"operation":${operation("op_plain")}}}"""

        /**
         * The cross-SDK golden vector dressed as a live proposal, so the armed
         * path exercises a digest that actually verifies rather than a
         * placeholder. [hopVenues] re-derives it before signing and would
         * reject anything else.
         */
        val PROPOSAL: String = proposalJson(Vectors.TO_VENUE)

        /**
         * The same proposal with a redirected destination — a server
         * describing one hop while asking for a signature over another.
         */
        val TAMPERED_PROPOSAL: String = proposalJson("0x000000000000000000000000000000000000dead")

        private fun proposalJson(toVenue: String): String = """{"success":true,"data":{
            "action":${CosignAction.TRANSFER_BETWEEN_VENUES},
            "boundaryId":"bnd_src","boundaryKey":"${Vectors.BOUNDARY}",
            "targetBoundaryId":"bnd_dst","targetBoundaryKey":"${Vectors.TO_BOUNDARY}",
            "fromVenue":"${Vectors.FROM_VENUE}","toVenue":"$toVenue",
            "toVenueAccountKey":"${Vectors.TO_VENUE_ACCOUNT}","token":"${Vectors.TOKEN}",
            "domain":{"name":"$COSIGN_DOMAIN_NAME","version":"$COSIGN_DOMAIN_VERSION",
                "chainId":${Vectors.CHAIN_ID},"verifyingContract":"${Vectors.KERNEL}"},
            "amount":"75.000000","amountRaw":"${Vectors.AMOUNT_RAW}",
            "ref":"${Vectors.REF}","nonce":"${Vectors.NONCE}","deadline":${Vectors.DEADLINE},
            "paramsHash":"${Vectors.PARAMS_HASH}","digest":"${Vectors.DIGEST}"}}"""

        val SUBMITTED = """{"success":true,"data":{
            "operation":${operation("op_hop")},
            "boundaryId":"bnd_src","targetBoundaryId":"bnd_dst"}}"""

        const val COSIGN_REFUSAL = """{"success":false,"error":{
            "code":"COSIGN_REQUIRED",
            "message":"The source exchange object's boundary requires user co-signed value-out operations.",
            "details":{"surface":"transfer.venue_hop","boundaryId":"bnd_src",
                "sourceArcaPath":"/users/a/exchange/hl","targetArcaPath":"/users/b/exchange/paper",
                "propose":"/api/v1/custody/venue-hops/propose","submit":"/api/v1/custody/venue-hops"}}}"""

        val NONCE_USED_REFUSAL = """{"success":false,"error":{
            "code":"COSIGN_NONCE_USED",
            "message":"co-sign nonce has already been used; re-propose the action",
            "details":{"boundaryId":"bnd_src","nonce":"${Vectors.NONCE}","reason":"nonce_consumed",
                "resolution":"re-propose the action to obtain a fresh nonce, re-sign, and resubmit"}}}"""

        const val NONCE_STATE_CONSUMED = """{"success":true,"data":{
            "boundaryId":"bnd_v7","nonce":"9223372036854775807",
            "spendable":false,"consumed":true,"unordered":true}}"""

        const val NONCE_STATE_COUNTER = """{"success":true,"data":{
            "boundaryId":"bnd_k5","nonce":"8",
            "spendable":false,"consumed":false,"unordered":false,"counterNonce":"9"}}"""
    }
}
