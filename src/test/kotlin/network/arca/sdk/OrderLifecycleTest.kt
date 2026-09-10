package network.arca.sdk
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
class OrderLifecycleTest {
 private val wire = """{"intent":{"realmId":"rlm_01h2xcejqtf2nbrexx3vqjhp41","objectId":"account","operationId":"original","leg":"0","venue":"gll-testnet","venueAccountId":"123","market":"gllt:3","requestedSize":"9007199254740993.123456789","orderType":"MARKET","side":"buy","timeInForce":"GTC","executionTimeInForce":"IOC","isTrigger":false,"isMarketTrigger":false,"sizeToMax":false,"reduceOnly":false},"venueOrderId":"3:order","submission":"accepted","working":false,"execution":"partial","terminal":true,"executedSize":"3.123456789","executionQuantityFinal":true,"requestedSizeKnown":true,"remainingSize":"9007199254740990","remainingDisposition":"canceled","accountedSize":"0","accountingComplete":false,"averagePrice":"2000.000000001","averagePriceFinal":false,"recoveryRequired":false}"""
 @Test fun preservesExactServerEvidenceAndAccount() {
  val v = Json.decodeFromString<OrderLifecycle>(wire)
  v.validate("rlm_01h2xcejqtf2nbrexx3vqjhp41", "account", "original", 0)
  assertEquals("9007199254740993.123456789", v.intent.requestedSize)
  assertEquals("3.123456789", v.executedSize)
  assertEquals("9007199254740990", v.remainingSize)
  assertFalse(v.working); assertFalse(v.accountingComplete); assertFalse(v.averagePriceFinal)
  assertThrows(IllegalArgumentException::class.java) { v.validate("rlm_01h2xcejqtf2nbrexx3vqjhp41", "foreign", "original", 0) }
  assertThrows(IllegalArgumentException::class.java) { v.validate("rlm_01h2xcejqtf2nbrexx3vqjhp41", "account", "foreign", 0) }
 }
 @Test fun missingFinalityCannotDecodeAsCompleteOrKnown() {
  val bad=wire.replace(""""executionQuantityFinal":true,""", "")
  assertThrows(Exception::class.java) { Json.decodeFromString<OrderLifecycle>(bad) }
 }
}
