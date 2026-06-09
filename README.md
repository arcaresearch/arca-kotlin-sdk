# Arca SDK — Kotlin/Android SDK for the Arca Platform

A Kotlin client for the [Arca](https://arcaos.io) financial infrastructure
platform — accounts, payments, perpetuals trading, real-time streaming, and
audit trails. Built on coroutines (`suspend` functions + `Flow`/`StateFlow`),
kotlinx.serialization, and OkHttp. A hand-written port of the Swift SDK with
full feature parity.

The library compiles to **JVM 1.8 bytecode**, so it is consumable from Android
(minSdk 24+) and any JVM 8+ runtime.

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
