package network.arca.sdk

import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.EventType
import network.arca.sdk.models.RealmEvent
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ExchangeProvisioningTest {

    /**
     * The wire strings are the contract with the server. A typo here means the
     * client never sees the event, which is indistinguishable from the account
     * never becoming ready.
     */
    @Test
    fun eventTypeWireStrings() {
        assertEquals("exchange.provisioned", EventType.EXCHANGE_PROVISIONED.wire)
        assertEquals("exchange.ready", EventType.EXCHANGE_READY.wire)
        assertEquals("deposit.detected", EventType.DEPOSIT_DETECTED.wire)
        assertEquals(EventType.EXCHANGE_READY, EventType.fromWire("exchange.ready"))
    }

    /**
     * A provisioned event on a cosign-armed boundary is the case the two-event
     * split exists for: the account is usable for reads but cannot trade until
     * the user co-signs, so the two flags must decode independently.
     */
    @Test
    fun decodesProvisionedOnArmedBoundary() {
        val json = """
            {
              "type": "exchange.provisioned",
              "entityId": "obj_1",
              "entityPath": "/users/alice/exchange",
              "exchange": {
                "objectId": "obj_1",
                "path": "/users/alice/exchange",
                "cosignRequired": true,
                "tradable": false,
                "accountAddress": "0x8f2a"
              }
            }
        """.trimIndent()

        val event = arcaJson.decodeFromString(RealmEvent.serializer(), json)
        val exchange = requireNotNull(event.exchange) {
            "the payload must decode, or the event arrives with nothing actionable on it"
        }
        assertEquals(true, exchange.cosignRequired)
        assertEquals(false, exchange.tradable)
        assertEquals("0x8f2a", exchange.accountAddress)
    }

    @Test
    fun decodesReady() {
        val json = """{"type":"exchange.ready","entityId":"obj_1","exchange":{"tradable":true,"cosignRequired":false}}"""
        val event = arcaJson.decodeFromString(RealmEvent.serializer(), json)
        assertEquals(true, event.exchange?.tradable)
    }

    @Test
    fun decodesDetectedDeposit() {
        val json = """{"type":"deposit.detected","deposit":{"address":"0xabc","amount":"15","sweeping":true}}"""
        val event = arcaJson.decodeFromString(RealmEvent.serializer(), json)
        assertEquals("15", event.deposit?.amount)
        assertEquals(true, event.deposit?.sweeping)
    }
}
