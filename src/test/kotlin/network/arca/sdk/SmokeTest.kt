package network.arca.sdk

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import network.arca.sdk.internal.SdkInfo
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SmokeTest {
    @Test
    fun serializationRoundTrips() {
        val json = Json.encodeToString(SdkInfo.serializer(), SdkInfo("arca-sdk", "0.1.0"))
        assertEquals("""{"name":"arca-sdk","version":"0.1.0"}""", json)
    }

    @Test
    fun coroutinesRun() = runTest {
        assertEquals(2, listOf(1, 1).sum())
    }
}
