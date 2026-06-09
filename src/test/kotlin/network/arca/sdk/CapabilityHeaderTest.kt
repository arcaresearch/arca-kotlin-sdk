package network.arca.sdk

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * Pins that every REST request advertises the SDK's client capabilities via the
 * `X-Arca-Client-Capabilities` header — the always-on half of the
 * server-authoritative-pricing contract (the server ignores it today).
 */
class CapabilityHeaderTest {

    @Serializable
    data class Probe(val ok: Boolean)

    private lateinit var server: MockWebServer

    @BeforeEach
    fun setUp() {
        server = MockWebServer()
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    @Test
    fun restRequestsAdvertiseClientCapabilitiesHeader() = runBlocking {
        server.enqueue(
            MockResponse()
                .setHeader("Content-Type", "application/json")
                .setBody("""{"success":true,"data":{"ok":true}}"""),
        )

        val client = ArcaClient(
            token = "jwt",
            baseUrl = server.url("/").toString().trimEnd('/'),
            httpClient = OkHttpClient(),
        )

        val probe: Probe = client.get("/probe")
        assertTrue(probe.ok)

        val recorded = server.takeRequest()
        val header = recorded.getHeader(ArcaClient.CLIENT_CAPABILITIES_HEADER)
        assertNotNull(header, "every REST request must carry X-Arca-Client-Capabilities")
        assertTrue(
            header!!.contains("server-authoritative-pricing"),
            "the server-authoritative-pricing capability must be advertised",
        )
        assertEquals(ArcaClient.ADVERTISED_CAPABILITIES.joinToString(","), header)
    }
}
