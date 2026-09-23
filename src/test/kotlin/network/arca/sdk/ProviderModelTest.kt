package network.arca.sdk

import kotlinx.serialization.decodeFromString
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.ArcaObjectDetailResponse
import network.arca.sdk.models.ArcaObjectType
import network.arca.sdk.models.DeltaType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class ProviderModelTest {

    @Test
    fun providerTypeAndDeltaTypesDecode() {
        assertEquals(ArcaObjectType.PROVIDER, arcaJson.decodeFromString<ArcaObjectType>("\"provider\""))
        assertEquals(DeltaType.PROVIDER_CHANGE, arcaJson.decodeFromString<DeltaType>("\"provider_change\""))
        assertEquals(DeltaType.DEPOSIT_LINK_CHANGE, arcaJson.decodeFromString<DeltaType>("\"deposit_link_change\""))
    }

    /**
     * Object detail of a provider object: an unobserved wallet (observation
     * null, never zero) and a link whose requested, observed and operation
     * state are separate fields.
     */
    @Test
    fun objectDetailWithProviderAndDepositLinksDecodes() {
        val json = """
        {"object":{"id":"obj_p","realmId":"rlm_1","path":"/users/a/privy","type":"provider","status":"active","systemOwned":false,"createdAt":"t","updatedAt":"t"},
         "operations":[],"events":[],"deltas":[],"balances":[],
         "provider":{"objectId":"obj_p","path":"/users/a/privy",
           "state":{"schema":1,"provider":"privy","subject":"did:privy:a","connection":{"status":"verified","checkedAt":"t","evidence":{"kind":"privy_identity_token","issuedAt":"t","expiresAt":"t"}}},
           "wallets":[{"walletId":"pwl_a","chainType":"ethereum","address":"0xA","role":"primary_deposit","verification":{"status":"verified"},"controls":[{"target":"pwl_b","relation":"signer_for"}],"observation":null}]},
         "depositLinks":[{"id":"dlk_1","realmId":"rlm_1",
           "source":{"objectId":"obj_p","path":"/users/a/privy","walletId":"pwl_a","address":"0xA"},
           "destination":{"objectId":"obj_c","path":"/users/a/cash","boundaryId":"0xb","account":{"kernel":"0xk","venue":"0xv","localId":"0xl"}},
           "chainId":"999","token":"0xt","adapter":{"address":"0xad","kind":"cash_autodeposit_v1"},
           "requested":{"state":"revoked","at":"t"},"lifetime":"until_revoked_or_invalidated","consent":null,"limits":null,
           "observed":{"status":"active","matchesDestination":true,"asOf":{"block":46200000}},
           "operations":{"revokeOperationId":"opr_1"},"progress":"revoke_in_flight","createdAt":"t","updatedAt":"t"}]}
        """.trimIndent()
        val detail = arcaJson.decodeFromString<ArcaObjectDetailResponse>(json)
        assertEquals(ArcaObjectType.PROVIDER, detail.`object`.type)
        val provider = assertNotNull(detail.provider).let { detail.provider!! }
        assertEquals("did:privy:a", provider.state.subject)
        assertNull(provider.wallets[0].observation)
        assertEquals("signer_for", provider.wallets[0].controls[0].relation)
        val link = detail.depositLinks!!.single()
        assertEquals("revoked", link.requested.state)
        assertEquals("active", link.observed.status)
        assertEquals("opr_1", link.operations.revokeOperationId)
        assertEquals("revoke_in_flight", link.progress)
        assertNull(link.consent)
    }

    @Test
    fun objectDetailWithoutProviderSectionsDecodes() {
        val json = """{"object":{"id":"obj_w","realmId":"rlm_1","path":"/w","type":"denominated","status":"active","systemOwned":false,"createdAt":"t","updatedAt":"t"},"operations":[],"events":[],"deltas":[],"balances":[]}"""
        val detail = arcaJson.decodeFromString<ArcaObjectDetailResponse>(json)
        assertNull(detail.provider)
        assertNull(detail.depositLinks)
    }
}
