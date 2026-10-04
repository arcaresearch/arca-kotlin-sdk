# Arca SDK — Kotlin/Android SDK for the Arca Platform

A Kotlin client for the [Arca](https://arcaos.io) financial infrastructure
platform — accounts, payments, perpetuals trading, real-time streaming, and
audit trails. Built on coroutines (`suspend` functions + `Flow`/`StateFlow`),
kotlinx.serialization, and OkHttp. A hand-written port of the Swift SDK with
full feature parity.

The library compiles to **JVM 1.8 bytecode**, so it is consumable from Android
(minSdk 24+) and any JVM 8+ runtime.

## Execution receipts and recorded prices

`OrderHandle.executionReceipt(...)` confirms terminal execution from account-scoped
pushes and the original operation. It preserves requested/executed/remainder quantities
for partially filled IOC orders even when venue history rewrites the order size.
Execution proof is independent of complete order metadata or journal settlement.
A venue aggregate price is provisional and may be absent.

Use the receipt's account-scoped `watchFills` merged list to refine the price:

```kotlin
val receipt = order.executionReceipt(timeoutSeconds = 30.0)
val fills = arca.watchFills(objectId = receipt.objectId)
val refined = receipt.refined(fills.fills.value)
// Collect fills.fills and refine again; stop the owned watch when done.
fills.stop()
```

Refinement requires recorded fills whose `orderOperationId` and `orderId` match
this receipt and whose deduplicated quantities equal its executed quantity.
`Fill.operationId` identifies the recording operation; previews cannot finalize a
price. Incomplete, conflicting, or foreign evidence leaves the receipt unchanged.
A complete result sets `averagePriceFinal`, `fillsComplete`, and
`averagePriceSource = "ledger_vwap"`; its VWAP is rounded to 18 fractional digits,
half-even. Original execution and remainder fields never change.

Fill events use the exact account `entityPath` when present; `entityId` may be a
fill ID. Object-ID matching is a fallback only for events without a path. Use
v2.3.1 or later for this account-scope correction.

`watchFills` installs listeners before subscribing, merges by stable `fillId`
(falling back to row `id`), and traverses history cursors up to 1,000 pages.
Its `limit` is the page size. Startup and actual gap/reconnect recovery use a fresh
watch acknowledgement plus a paginated snapshot, with at most three attempts per
recovery. Failed recovery remains `reconnecting` until a later gap/reconnect;
healthy watches perform no periodic history reads. Stop the watch on account
change. A fill has no embedded account ID, so callers must retain the account
scope of the watch used for refinement.

## Requirements

- A JDK 17+ to *build* (the produced artifact targets JVM 1.8).
- Android: `minSdkVersion 24+` and the `INTERNET` permission.

## Installation

The SDK is published via [JitPack](https://jitpack.io).

```kotlin
// settings.gradle.kts
dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
        maven { url = uri("https://jitpack.io") }
    }
}

// app/build.gradle.kts
dependencies {
    implementation("com.github.arcaresearch:arca-kotlin-sdk:v0.1.0")
}
```

Pin a specific release with its version tag (e.g. `v0.1.0`). The SDK pulls in
OkHttp, kotlinx.coroutines, and kotlinx.serialization transitively.

## Quick Start

```kotlin
import network.arca.sdk.*

// Initialize with automatic token refresh (recommended). The realm id is read
// from the JWT claims unless you pass realmId explicitly.
val arca = Arca(
    token = scopedJwt,
    tokenProvider = { myBackend.getArcaToken() },
)

// Ensure a denominated wallet exists, then fund it (dev/test only).
arca.ensureDenominatedArca(ref = "/wallets/main").settled()
arca.fundAccount(arcaRef = "/wallets/main", amount = "1000.00").settled()

// Transfer — the nonce path is the idempotency key.
val nonce = arca.nonce("/op/transfer/main-to-savings/001")
arca.transfer(
    path = nonce.path,
    from = "/wallets/main",
    to = "/wallets/savings",
    amount = "50",
).settled()

val balances = arca.getBalancesByPath("/wallets/savings")
println("Settled: ${balances.first().settled}")

// Release the WebSocket + coroutine scope when done.
arca.close()
```

## Conventions

- **Reads** are `suspend` functions. **Mutations** return a handle immediately;
  the request runs in the background. Await `submitted()` (HTTP accept) or
  `settled()` (terminal state).
- Money/amount values are **decimal strings** (`"50"`, `"0.01"`).
- Market ids are canonical `{exchange}:{id}` (`"hl:0:BTC"`, `"hl:1:TSLA"`),
  case-sensitive. Resolve a display symbol via `arca.resolveMarkets("BTC")`.
- Market-data timestamps are Unix epoch **milliseconds**; all others are RFC
  3339 UTC strings.

## Exchange (Perpetuals)

```kotlin
val order = arca.placeOrder(
    path = arca.nonce("/op/order/btc").path,
    objectId = exchangeObjectId,
    market = "hl:0:BTC",
    side = OrderSide.BUY,
    orderType = OrderType.LIMIT,
    size = "0.01",
    price = "50000",
    leverage = 5,
)
val filled = order.filled(timeoutSeconds = 30.0)
```

## Real-time Streaming

Watch factories open a shared WebSocket and return a stream exposing a
`StateFlow` snapshot plus a `Flow` of incremental `updates`. Call `ready()` to
suspend until the first snapshot and `stop()` when finished.

```kotlin
val watch = arca.watchObject("/wallets/main")
watch.ready()
val job = scope.launch {
    watch.updates.collect { v -> render(v.valueUsd) }
}
// ...later
job.cancel()
watch.stop()
```

The connection is reference-counted and self-heals across reconnects and app
background/foreground transitions.

### Wallet Account (V9 cash)

One read and one stream per owner-facing wallet, composed by Arca from durable
records with no chain call. The `Flow` delivers the *complete* Wallet Account on
connect and after every change (at most one per 250 ms), resumes with
`Last-Event-ID` and 1 s → 30 s backoff after a disconnect, and fails only when
the server refuses the connection.

```kotlin
val wallet = arca.walletAccount(boundaryId)

scope.launch {
    arca.walletAccountEvents(boundaryId).collect { wallet ->
        render(wallet) // wallet.typedWalletState, wallet.balances, wallet.autoDeposit, wallet.operations
    }
}
```

## Error Handling

All failures are subclasses of the sealed `ArcaException`:

```kotlin
try {
    arca.transfer(path = nonce.path, from = a, to = b, amount = "10").settled()
} catch (e: ArcaException) {
    when (e) {
        is ArcaException.Validation -> showFieldError(e.message)
        is ArcaException.Unauthorized -> reauthenticate()
        is ArcaException.OperationFailed -> showFailed(e.operation.state)
        else -> report(e)
    }
}
```

## Testing

```bash
./gradlew test
```

## License

MIT


## Account capabilities and reduction sizing

Use getExchangeCapabilities for account-authoritative optional controls. Use normalizedReductionSize with canonical market, exact size and fraction; it reads market lot precision and returns an exact rounded-down decimal, rejecting missing metadata and invalid/sub-lot values. Never infer precision or feature support from a venue prefix.
## Operation wait recovery

`waitForOperation` listens before acquiring its subscription. Startup and actual
stream gaps, reauthentication, or sparse operation notifications request a fresh,
correlated acknowledgement before reading the operation. A failed acknowledgement
or read gets at most three attempts per recovery; a healthy pending operation
stays on the stream without periodic reads. A terminal push can complete during
acknowledgement or snapshot recovery. Timeout stops the wait and preserves the
original operation identity; it never submits a replacement operation.

Terminal operations in the initial or buffered subscription snapshot resolve the wait before any HTTP read, including typed failed/expired results. Socket rotation requests fresh operation evidence on the replacement connection; its pong only establishes transport readiness. Stale snapshot request IDs and unrelated operation IDs cannot settle the wait.
Verified snapshot operations are shared with all live waiters, including results buffered while another caller refreshes the same root watch; each waiter still accepts only its original operation ID.

### Optional direct public market data (2.11.0)

The Swift and Kotlin SDKs keep **Arca as the default**. Opting into Hyperliquid
changes public midpoint prices and the current open candle only. Choose the
venue network explicitly; the SDK never infers it from a realm or custody chain.
Both sources feed the same SDK price stream, including client-priced equity,
P&L, valuation and sizing. Server-priced states, allocation quotes, limits,
execution and recorded fills retain their existing authority.

Direct quotes use **BBO midpoint, not Hyperliquid mark or last trade**. Raw
precision is preserved; round only for presentation. The adapter reads exact
`Market.venueSymbol` metadata for a canonical market ID. Missing or ambiguous
metadata, non-Hyperliquid markets, excess interests and 15-second candles stay
on Arca. `watchPrices()` without a market list does not subscribe the entire
venue directly. Pass the visible markets and replace them with `setMarkets` as
visibility changes. Exchange-state watches add their held markets automatically;
max-order-size watches add their selected market. Always stop unused watches.

One public socket per SDK instance has at most 64 subscriptions (price interests
first, then candles; each group sorted by canonical ID). It uses a separate
unauthenticated client, JSON heartbeats, bounded frames, paced subscription
changes and reconnect backoff. Background/disconnect and zero interests close
it. These are per-instance limits, not guarantees against shared-IP venue limits.

Direct price changes are merged before financial fan-out: first change immediate,
then at most one latest batch per 100 ms, with one pending value per market and
no idle timer. Quantity-only BBO changes do not trigger valuation. Do not add a
second 100 ms throttle downstream when consuming this path; retain per-field UI
deduplication and separate slower chart-history work. The scheduler bounds added
SDK delay under normal execution, not OS stalls or venue update cadence.

Arca stays subscribed. Each market uses Arca until its first valid direct frame;
on source failure it resumes from the next arriving Arca frame, never an emitted
cached fallback. Reconnection messages are generation-fenced. The existing Arca
wire payload lacks the venue event timestamp, so fallback event-time ordering
and exact end-to-end age cannot be guaranteed. `marketDataSourceStatus` reports
preference, requested direct subscription count, markets with direct frames, and
the latest source error; readiness is not a latency or completeness guarantee.

Direct candles update only the current open bucket. Arca finalized bars and
history/gap recovery remain authoritative; a closed bar cannot be overwritten by
a late direct open frame. No price or candle polling fallback is introduced.
Switch preference back to Arca at runtime without replacing watches; a
configuration-generation guard prevents a late metadata read from re-enabling
an earlier preference.

```kotlin
arca.setMarketDataPreference(MarketDataPreference.HYPERLIQUID, HyperliquidNetwork.MAINNET)
val prices = arca.watchPrices(markets = listOf("hl:0:BTC", "hl:0:ETH"))
prices.setMarkets(listOf("hl:0:BTC"))
val source = arca.marketDataSourceStatus
// Reversible, including existing account/valuation watches:
arca.setMarketDataPreference(MarketDataPreference.ARCA)
prices.stop()
```
