package network.arca.sdk

import kotlinx.coroutines.*
import kotlinx.serialization.json.*
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.Candle
import network.arca.sdk.models.CandleInterval
import okhttp3.*
import java.math.BigDecimal
import java.math.MathContext
import java.util.concurrent.TimeUnit
import kotlin.random.Random

/** Public-only adapter. It never receives Arca tokens, cookies or an authenticated HTTP client. */
internal class HyperliquidMarketSource(
    private val url: String,
    private val receive: (Long, PublicMarketUpdate) -> Unit,
    private val factory: WebSocket.Factory = publicClient,
    private val scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.Default),
) : PublicMarketSource {
    private val guard = Any()
    private var desired = emptySet<PublicMarketSubscription>()
    private var sent = emptySet<PublicMarketSubscription>()
    private var socket: WebSocket? = null
    private var open = false
    private var closed = false
    private var generation = 0L
    private var openedAtNs = 0L
    private var lastReceivedNs = 0L
    private var attempt = 0
    private var changes: Job? = null
    private var heartbeat: Job? = null
    private var retry: Job? = null

    override fun subscribe(subscriptions: Set<PublicMarketSubscription>) {
        synchronized(guard) {
            if (closed || subscriptions == desired) return
            desired = subscriptions
            if (desired.isEmpty()) { stopSocket(); retry?.cancel(); retry = null; return }
            if (socket == null && retry == null) connect()
            if (open) sync()
        }
    }
    private fun connect() {
        val epoch = ++generation
        open = false; sent = emptySet()
        socket = factory.newWebSocket(Request.Builder().url(url).build(), object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) = synchronized(guard) {
                if (closed || epoch != generation) { webSocket.cancel(); return }
                socket = webSocket; open = true
                openedAtNs = System.nanoTime(); lastReceivedNs = openedAtNs
                sync()
                heartbeat = scope.launch {
                    while (isActive) {
                        delay(25_000)
                        val stale = synchronized(guard) {
                            if (epoch != generation) return@launch
                            if (System.nanoTime() - lastReceivedNs > 45_000_000_000L) true
                            else !webSocket.send("{\"method\":\"ping\"}")
                        }
                        if (stale) { fail(epoch, "Hyperliquid heartbeat timed out"); return@launch }
                    }
                }
            }
            override fun onMessage(webSocket: WebSocket, text: String) {
                val subscriptions = synchronized(guard) {
                    if (closed || epoch != generation) return
                    lastReceivedNs = System.nanoTime()
                    sent
                }
                if (text.length > 262_144) { fail(epoch, "Hyperliquid frame exceeded size budget"); return }
                val json = runCatching { arcaJson.parseToJsonElement(text).jsonObject }.getOrNull() ?: return
                if ((json["channel"] as? JsonPrimitive)?.contentOrNull == "error") {
                    fail(epoch, "Hyperliquid subscription rejected")
                    return
                }
                decodePublicMarketUpdate(json, subscriptions)?.let { receive(epoch, it) }
            }
            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) { fail(epoch, "Hyperliquid connection failed") }
            override fun onClosing(webSocket: WebSocket, code: Int, reason: String) { fail(epoch, "Hyperliquid connection closed") }
            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) { fail(epoch, "Hyperliquid connection closed") }
        })
    }
    private fun sync() {
        if (changes?.isActive == true) return
        val epoch = generation
        changes = scope.launch {
            while (isActive) {
                val failed = synchronized(guard) {
                    if (closed || epoch != generation || !open) return@launch
                    val remove = (sent - desired).firstOrNull()
                    val add = (desired - sent).firstOrNull()
                    val sub = remove ?: add ?: run { changes = null; return@launch }
                    val message = buildJsonObject {
                        put("method", if (remove != null) "unsubscribe" else "subscribe")
                        putJsonObject("subscription") {
                            put("type", if (sub.interval == null) "bbo" else "candle")
                            put("coin", sub.coin)
                            sub.interval?.let { put("interval", it.wire) }
                        }
                    }
                    if (socket?.send(message.toString()) != true) true
                    else { sent = if (remove != null) sent - sub else sent + sub; false }
                }
                if (failed) { fail(epoch, "Hyperliquid subscription send failed"); return@launch }
                delay(100) // <=10 subscription messages/second, including churn
            }
        }
    }
    private fun fail(epoch: Long, reason: String) {
        val notify = synchronized(guard) {
            if (closed || epoch != generation) return
            if (open && System.nanoTime() - openedAtNs >= 60_000_000_000L) attempt = 0
            stopSocket()
            if (desired.isNotEmpty()) {
                val seconds = (2L shl attempt.coerceAtMost(5)).coerceAtMost(60)
                attempt = (attempt + 1).coerceAtMost(6)
                retry = scope.launch {
                    delay(seconds * 1000 + Random.nextLong(0, 500))
                    synchronized(guard) { retry = null; if (!closed && desired.isNotEmpty()) connect() }
                }
            }
            generation
        }
        receive(notify, PublicMarketUpdate.Unavailable(reason))
    }
    private fun stopSocket() {
        ++generation; open = false; sent = emptySet()
        changes?.cancel(); changes = null; heartbeat?.cancel(); heartbeat = null
        socket?.cancel(); socket = null
    }
    override fun close() {
        synchronized(guard) {
            closed = true; desired = emptySet(); stopSocket(); retry?.cancel(); retry = null
        }
        scope.cancel()
    }
    companion object {
        private val publicClient = OkHttpClient.Builder().connectTimeout(10, TimeUnit.SECONDS).readTimeout(0, TimeUnit.MILLISECONDS).build()
    }
}

/** Mapping is an exact reverse lookup of subscribed venue symbols, never a guessed canonical ID. */
internal fun decodePublicMarketUpdate(json: JsonObject, subscriptions: Set<PublicMarketSubscription>): PublicMarketUpdate? = runCatching {
    val channel = json["channel"]?.jsonPrimitive?.contentOrNull
    val data = json["data"]?.jsonObject ?: return null
    when (channel) {
        "bbo" -> {
            val coin = data["coin"]?.jsonPrimitive?.contentOrNull ?: return null
            val sub = subscriptions.singleOrNull { it.coin == coin && it.interval == null } ?: return null
            val levels = data["bbo"]?.jsonArray ?: return null
            if (levels.size != 2) return null
            val bid = levels[0].jsonObject["px"]!!.jsonPrimitive.content.marketDecimal() ?: return null
            val ask = levels[1].jsonObject["px"]!!.jsonPrimitive.content.marketDecimal() ?: return null
            if (bid.signum() <= 0 || ask < bid) return null
            val price = bid.add(ask).divide(BigDecimal(2), MathContext.DECIMAL128).stripTrailingZeros().toPlainString()
            PublicMarketUpdate.Quote(sub.market, price, data["time"]!!.jsonPrimitive.long)
        }
        "candle" -> {
            val coin = data["s"]?.jsonPrimitive?.contentOrNull ?: return null
            val interval = CandleInterval.fromWire(data["i"]?.jsonPrimitive?.contentOrNull ?: return null) ?: return null
            val sub = subscriptions.singleOrNull { it.coin == coin && it.interval == interval } ?: return null
            // Hyperliquid's s is the symbol; Arca Candle.s is provenance. Never copy it across.
            val candle = arcaJson.decodeFromJsonElement<Candle>(JsonObject(data.filterKeys { it in setOf("t", "o", "h", "l", "c", "v", "n") }))
            val numbers = listOf(candle.o, candle.h, candle.l, candle.c).map { it.marketDecimal() ?: return null }
            val volume = candle.v.marketDecimal() ?: return null
            if (numbers.any { it.signum() <= 0 } || volume.signum() < 0 || candle.n < 0 ||
                numbers[1] < numbers.maxOrNull() || numbers[2] > numbers.minOrNull()) return null
            PublicMarketUpdate.Bar(sub.market, interval, candle)
        }
        else -> null
    }
}.getOrNull()

private val marketNumber = Regex("[0-9]+(?:\\.[0-9]+)?")
private fun String.marketDecimal(): BigDecimal? =
    if (length <= 48 && marketNumber.matches(this)) toBigDecimalOrNull() else null
