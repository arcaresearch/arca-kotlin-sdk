package network.arca.sdk

import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.Market
import kotlinx.serialization.json.jsonObject
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MarketDataBudgetTest {
    private fun router(native: Int = 20, xyz: Int = 3): MarketDataRouter {
        val markets = (0 until native).map { "hl:0:C$it" to "C$it" } + (0 until xyz).map { "hl:1:C$it" to "xyz:C$it" }
        return MarketDataRouter().apply {
            configure(MarketDataPreference.HYPERLIQUID, markets.map { (id, coin) -> arcaJson.decodeFromString<Market>(
                """{"name":"$id","venueSymbol":"$coin","symbol":"same","exchange":"hl","index":0,"szDecimals":5,"maxLeverage":50,"onlyIsolated":false}""") })
            register("watch"); update("watch", markets.map { it.first }.toSet(), 0)
        }
    }
    @Test fun weakDeviceKeepsCoarseCoverageDespiteFastConnection() {
        val r = router(); r.setBudget(MarketDataBudget(100_000.0, 0.4, mapOf("hl:0:C0" to 100)))
        assertEquals(setOf("", "xyz"), r.subscriptions.mapNotNull { it.dex }.toSet())
        assertEquals(2, r.subscriptions.size)
    }
    @Test fun highestVisibleDemandWinsAndTheOtherDexKeepsCoarseCoverage() {
        val r = router(); r.setBudget(MarketDataBudget(6_300.0, 8.0, mapOf("hl:0:C9" to 100, "hl:0:C0" to 10)))
        assertEquals(setOf("hl:0:C9"), r.subscriptions.filter { it.dex == null }.map { it.market }.toSet())
        assertEquals(3, r.subscriptions.size)
    }
    @Test fun smallScopeUsesBboOnlyWhenCheaperAndReceiveBudgetAllowsIt() {
        val r = router(native = 2); r.setBudget(MarketDataBudget(2_000.0, 9.0))
        assertEquals(setOf("hl:0:C0", "hl:0:C1"), r.subscriptions.filter { it.dex == null }.map { it.market }.toSet())
        assertEquals(listOf("xyz"), r.subscriptions.mapNotNull { it.dex })
    }
    @Test fun allMidsIsOneCanonicalBatchAndNeverReplaysOverAnEstablishedBbo() {
        val r = router(); r.setBudget(MarketDataBudget(6_300.0, 8.0, mapOf("hl:0:C0" to 100)))
        val mids = PublicMarketUpdate.Mids("", mapOf("C0" to "100", "C1" to "200", "xyz:C0" to "999", "UNKNOWN" to "10"))
        assertEquals(mapOf("hl:0:C0" to "100", "hl:0:C1" to "200"), r.direct(mids, 1000)?.mids)
        r.direct(PublicMarketUpdate.Quote("hl:0:C0", "101", 1000), 1000)
        assertEquals(mapOf("hl:0:C1" to "200"), r.direct(mids, 90_000)?.mids, "quiet BBO is still current; allMids has no exchange timestamp")
        r.setBudget(MarketDataBudget(100_000.0, 0.4))
        assertEquals("100", r.direct(mids, 90_001)?.mids?.get("hl:0:C0"))
        r.release("watch")
        assertNull(r.direct(mids, 90_002)); assertTrue(r.subscriptions.isEmpty())
    }
    @Test fun expensiveMeasuredFeedYieldsBudgetToCheaperVisibleAsset() {
        val r = router(xyz = 0); r.setBudget(MarketDataBudget(5_700.0, 12.0, mapOf("hl:0:C0" to 100, "hl:0:C1" to 90)))
        r.direct(PublicMarketUpdate.Cost("hl:0:C0", 200_000), 0)
        r.direct(PublicMarketUpdate.Cost("hl:0:C0", 100), 10_000)
        assertEquals(setOf("hl:0:C1"), r.subscriptions.filter { it.dex == null }.map { it.market }.toSet())
        r.setBudget(null)
        assertEquals(20, r.subscriptions.size)
    }
    @Test fun bestBudgetCanPromoteEveryRequiredAssetIncludingPassiveOnes() {
        val r = router(); r.setBudget(MarketDataBudget(100_000.0, 200.0))
        assertEquals(23, r.subscriptions.size); assertTrue(r.subscriptions.all { it.dex == null })
    }
    @Test fun malformedAndUnrequestedMidsNeverReachTheCanonicalBus() {
        fun decode(text: String) = decodePublicMarketUpdate(arcaJson.parseToJsonElement(text).jsonObject, setOf(PublicMarketSubscription("", "", dex = "xyz")))
        assertNull(decode("""{"channel":"allMids","data":{"mids":{"BTC":"1"}}}"""))
        val result = decode("""{"channel":"allMids","data":{"dex":"xyz","mids":{"xyz:C0":"10","bad":"NaN","zero":"0","exponent":"1e100"}}}""") as PublicMarketUpdate.Mids
        assertEquals(mapOf("xyz:C0" to "10"), result.prices)
    }
}
