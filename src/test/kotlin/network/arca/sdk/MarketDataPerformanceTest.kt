package network.arca.sdk

import kotlinx.serialization.json.*
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import java.lang.management.ManagementFactory

/** Opt-in host CPU replay; not a device battery/rendering claim. */
class MarketDataPerformanceTest {
    @Test fun benchmarkDecodedPricesWithEquityAndSizing() {
        assumeTrue(System.getenv("ARCA_MARKET_BENCHMARK") == "1")
        val base = arcaJson.decodeFromString<ExchangeState>("""{"account":{"id":"a1","realmId":"r1","name":"main","createdAt":"2026-09-06","updatedAt":"2026-09-06"},"marginSummary":{"equity":"1000","initialMarginUsed":"150","maintenanceMarginRequired":"0","availableToWithdraw":"950","totalNtlPos":"3000","totalUnrealizedPnl":"0","totalRawUsd":"1000"},"positions":[{"id":"p0","market":"hl:0:C0","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p1","market":"hl:0:C1","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p2","market":"hl:0:C2","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p3","market":"hl:0:C3","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p4","market":"hl:0:C4","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p5","market":"hl:0:C5","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p6","market":"hl:0:C6","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p7","market":"hl:0:C7","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p8","market":"hl:0:C8","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p9","market":"hl:0:C9","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p10","market":"hl:0:C10","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p11","market":"hl:0:C11","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p12","market":"hl:0:C12","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p13","market":"hl:0:C13","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p14","market":"hl:0:C14","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p15","market":"hl:0:C15","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p16","market":"hl:0:C16","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p17","market":"hl:0:C17","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p18","market":"hl:0:C18","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p19","market":"hl:0:C19","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p20","market":"hl:0:C20","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p21","market":"hl:0:C21","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p22","market":"hl:0:C22","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p23","market":"hl:0:C23","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p24","market":"hl:0:C24","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p25","market":"hl:0:C25","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p26","market":"hl:0:C26","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p27","market":"hl:0:C27","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p28","market":"hl:0:C28","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"},{"id":"p29","market":"hl:0:C29","side":"long","size":"1","entryPrice":"100","leverage":20,"marginUsed":"50","positionValue":"1000","unrealizedPnl":"0"}],"openOrders":[]}""")
        val subs = (0..<30).map { PublicMarketSubscription("hl:0:C$it", "C$it") }.toSet()
        val frames = (0..<3000).map { n -> """{"channel":"bbo","data":{"coin":"C${n % 30}","time":${1000 + n},"bbo":[{"px":"${100 + n / 30}"},{"px":"${101 + n / 30}"}]}}""" }
        val cpu = ManagementFactory.getThreadMXBean()
        fun run(batch: Int): Pair<Long, ExchangeState> {
            val mids = mutableMapOf<String, String>(); var last = base
            val start = cpu.currentThreadCpuTime
            frames.forEachIndexed { i, text ->
                val update = decodePublicMarketUpdate(arcaJson.parseToJsonElement(text).jsonObject, subs) as PublicMarketUpdate.Quote
                mids[update.market] = update.price
                if ((i + 1) % batch == 0) {
                    last = base.revalued(mids)
                    check(deriveActiveAssetData(last, "hl:0:C0", mids["hl:0:C0"]!!.toDouble(), 20, OrderSide.BUY) != null)
                }
            }
            return (cpu.currentThreadCpuTime - start) to last
        }
        run(1); run(30)
        val raw = run(1); val batched = run(30)
        assertEquals(raw.second.marginSummary.equity, batched.second.marginSummary.equity)
        println("MARKET_BENCH kotlin frames=3000 positions=30 raw_cpu_ms=${raw.first / 1e6} batched_cpu_ms=${batched.first / 1e6} raw_revaluations=3000 batched_revaluations=100 final_equity=${batched.second.marginSummary.equity}")
    }
}
