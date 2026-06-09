package network.arca.sdk

import network.arca.sdk.models.Channel
import network.arca.sdk.models.EventType
import network.arca.sdk.models.OrderSide
import network.arca.sdk.models.OrderType
import network.arca.sdk.models.TimeInForce
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.Base64

class ArcaTest {

    private fun base64Url(s: String): String =
        Base64.getUrlEncoder().withoutPadding().encodeToString(s.toByteArray())

    private fun token(payloadJson: String): String =
        "${base64Url("""{"alg":"HS256","typ":"JWT"}""")}.${base64Url(payloadJson)}.fakesignature"

    // MARK: - JWT decoding

    @Test
    fun initWithValidToken() {
        val arca = Arca(token = token("""{"realmId":"rlm_test123","sub":"usr_abc"}"""))
        assertEquals("rlm_test123", arca.realm)
    }

    @Test
    fun initWithExplicitRealmId() {
        val arca = Arca(token = token("""{"sub":"usr_abc"}"""), realmId = "rlm_explicit")
        assertEquals("rlm_explicit", arca.realm)
    }

    @Test
    fun initFailsWithoutRealmId() {
        val thrown = runCatching { Arca(token = token("""{"sub":"usr_abc"}""")) }.exceptionOrNull()
        val v = thrown as? ArcaException.Validation
        assertNotNull(v)
        assertTrue(v?.message?.contains("realmId") == true, "got: ${v?.message}")
    }

    @Test
    fun initFailsWithInvalidJwt() {
        val thrown = runCatching { Arca(token = "not-a-jwt") }.exceptionOrNull()
        val v = thrown as? ArcaException.Validation
        assertNotNull(v)
        assertTrue(v?.message?.contains("JWT") == true, "got: ${v?.message}")
    }

    // MARK: - Event types

    @Test
    fun eventTypeWireValues() {
        assertEquals("operation.created", EventType.OPERATION_CREATED.wire)
        assertEquals("operation.updated", EventType.OPERATION_UPDATED.wire)
        assertEquals("balance.updated", EventType.BALANCE_UPDATED.wire)
        assertEquals("exchange.updated", EventType.EXCHANGE_UPDATED.wire)
        assertEquals("aggregation.updated", EventType.AGGREGATION_UPDATED.wire)
        assertEquals("mids.updated", EventType.MIDS_UPDATED.wire)
    }

    @Test
    fun channelWireValues() {
        assertEquals("operations", Channel.OPERATIONS.wire)
        assertEquals("balances", Channel.BALANCES.wire)
        assertEquals("exchange", Channel.EXCHANGE.wire)
        assertEquals("objects", Channel.OBJECTS.wire)
        assertEquals("agent", Channel.AGENT.wire)
    }

    // MARK: - Exchange enums

    @Test
    fun orderSideWireValues() {
        assertEquals("buy", OrderSide.BUY.wire)
        assertEquals("sell", OrderSide.SELL.wire)
    }

    @Test
    fun orderTypeWireValues() {
        assertEquals("MARKET", OrderType.MARKET.wire)
        assertEquals("LIMIT", OrderType.LIMIT.wire)
    }

    @Test
    fun timeInForceWireValues() {
        assertEquals("GTC", TimeInForce.GTC.wire)
        assertEquals("IOC", TimeInForce.IOC.wire)
        assertEquals("ALO", TimeInForce.ALO.wire)
    }
}
