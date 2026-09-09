package network.arca.sdk

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class OrderSizingTest {
    @Test fun exactDownwardLots() {
        assertEquals("0.08776", normalizedReductionSize("0.17552", "0.5", 5))
        assertEquals("0.08776", normalizedReductionSize("0.17553", "0.5", 5))
        assertEquals("123456789.123456789", normalizedReductionSize("123456789.123456789", "1", 9))
        assertEquals("2", normalizedReductionSize("5", "0.5", 0))
    }
    @Test fun invalidAndSubLotInputsFailClosed() {
        for (size in listOf("0", "-1", "NaN", "1junk", "1e9", " 1"))
            assertThrows(IllegalArgumentException::class.java) { normalizedReductionSize(size, "1", 5) }
        for (fraction in listOf("0", "-1", "1.1", "NaN"))
            assertThrows(IllegalArgumentException::class.java) { normalizedReductionSize("1", fraction, 5) }
        assertThrows(IllegalArgumentException::class.java) { normalizedReductionSize("0.00001", "0.5", 5) }
        assertThrows(IllegalArgumentException::class.java) { normalizedReductionSize("1", "1", -1) }
    }
}

class OrderSizingHTTPTest {
    @Test fun canonicalMetadataAndCapabilities() = kotlinx.coroutines.runBlocking {
        val server = okhttp3.mockwebserver.MockWebServer()
        server.start()
        try {
            val enc = java.util.Base64.getUrlEncoder().withoutPadding()
            val token = enc.encodeToString("{}".toByteArray()) + "." + enc.encodeToString("""{"realmId":"realm"}""".toByteArray()) + ".fixture"
            val arca = Arca(token = token, baseUrl = server.url("/").toString().trimEnd('/'))
            server.enqueue(okhttp3.mockwebserver.MockResponse().setBody("""{"success":true,"data":{"universe":[{"name":"hl:0:BTC","symbol":"BTC","exchange":"hl","index":0,"szDecimals":5,"maxLeverage":50,"onlyIsolated":false},{"name":"gllt:13","symbol":"SP500","exchange":"gll","index":13,"szDecimals":3,"maxLeverage":10,"onlyIsolated":false}]}}"""))
            assertEquals("0.08776", arca.normalizedReductionSize("hl:0:BTC", "0.17553", "0.5"))
            assertEquals("0.087", arca.normalizedReductionSize("gllt:13", "0.17553", "0.5"))
            try { arca.normalizedReductionSize("missing", "1", "1"); fail<String>("Missing market must fail") } catch (_: IllegalArgumentException) {}
            val capability = """{"success":true,"data":{"objectId":"account","orderTypes":["MARKET","LIMIT"],"timeInForce":["GTC","IOC"],"marginModes":["cross"],"leverageSelection":true,"positionTriggers":false,"brackets":false,"orderKeyRetirement":true}}"""
            server.enqueue(okhttp3.mockwebserver.MockResponse().setBody(capability))
            assertFalse(arca.getExchangeCapabilities("account").brackets)
            server.enqueue(okhttp3.mockwebserver.MockResponse().setBody(capability))
            try { arca.getExchangeCapabilities("different"); fail<String>("Wrong account must fail") } catch (_: IllegalArgumentException) {}
            assertEquals(3, server.requestCount)
        } finally { server.shutdown() }
    }
}
