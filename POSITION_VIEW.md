# Receipt-backed position display

`PositionView` is a shared, in-memory display view owned by one Arca client and
exchange object. It changes visible **quantity and presence** immediately when a
tracked order handle returns confirmed terminal execution. It does not change
`ExchangeState`, financial positions, balances, P&L, fees, margin, collateral,
allocation, order sizing, close eligibility, or `accounted()`'s ledger meaning.

## Native integration

Swift:

```swift
let watch = try await arca.watchExchangeState(objectId: accountId)
let view = arca.positionView(objectId: accountId)
let observation = view.current.onChange { snapshot in /* render display rows */ }
// Read view.current.value after subscribing for the initial value.

// Once per logical order, BEFORE its first backend POST:
let update = try view.begin(market: market, side: side)
let result = try await backend.placeOrder(/* stable idempotency key */)
let order = try await arca.orderHandle(objectId: accountId, operationId: result.operationId)
try await order.trackPositionUpdate(update)
let receipt = try await order.executionReceipt()
// view.current already includes this execution; now dismiss the receipt.
// Retain the handle and scope in account-owned work after dismissal:
_ = try await order.accounted()
```

Kotlin uses the same sequence: `arca.positionView(accountId)`,
`view.begin(market, side)`, `order.trackPositionUpdate(update)`,
`order.executionReceipt()`, and `order.accounted()`. Observe `view.current`, a
conflated `StateFlow<PositionViewSnapshot>`, and its initial `.value`.

An existing exchange watch automatically creates and feeds this same view,
including its initial snapshot and subsequent structural and market updates.
Creating the view after the watch starts returns that existing view. Keep the
watch alive for the account. A direct `getExchangeState()` alone does not feed it.

TypeScript uses `arca.positionView(accountId)` and `view.subscribe(callback)`
(which immediately delivers the initial value). It has the same `begin` and
`trackPositionUpdate` contract; its existing `order.filled()` is the execution
boundary, and `accounted()` is the accounting boundary. TypeScript does not gain
a native-style executionReceipt API in this release.

## Render contract

- `positions`: `market`, exact decimal `signedSize`, `source`, and optional
  `authoritativePosition`. Sources are `authoritative`, `baseline` (reserved,
  execution not observed yet), and `execution`. The latter two carry no
  authoritative position object. Positive quantity is long; negative is short.
  Zero has no row. Projected entry price and all financial fields are unknown.
- `coverage`: original `operationId`, venue `orderId`, `market`, cumulative
  `filledSize`, and status `execution`, `accounted`, or `unavailable`.
  Coverage survives a full close even though its position row disappears.
  Match operation AND account; do not infer coverage merely from a missing row.
- `pendingMarkets`: every reserved market, including confirmed full closes.
- `unavailableMarkets`: insufficient evidence. A missing row here is **unknown**,
  not a proven flat position. Coverage with `unavailable` must not clear feedback
  claiming the position change has been reflected.

Keep authoritative financial collections separate. Do not convert a display row
into a financial position by supplying zero for missing fields. The SDK does not
provide a new projected entry-price/P&L calculation in this release.

## Baseline, retries, and lifetime

`begin` requires an available coherent observation with a UTC
`tradingAllocation.asOf`; expired observations, ambiguous markets, malformed
quantities, and the 128-active-scope limit fail with
`POSITION_BASELINE_UNAVAILABLE` (Swift ArcaError, Kotlin IllegalStateException,
TypeScript Error). This currently targets the dated Arca mirror state. An
undated venue state is not sufficient for safe snapshot ordering.

Use one scope for one logical order. Retain and reuse it across same-process
transport retries and read-only recovery. A scope is not persistable/exportable.
After relaunch, a lost scope, an unsupported baseline, or legacy recovery, keep
that order authoritative-only. Never capture a replacement baseline after its
POST. A new local order gets its own scope; the shared view combines quantities
against the first still-pending baseline for that market. Operation identity,
account, market, side, and creation time relative to the baseline are checked.
Sharing one operation ID between bracket legs is not supported by this contract.

An attach/track/receipt error does not cancel or downgrade an already-confirmed
backend fill. Call `update.invalidate()` to mark display evidence unavailable,
retain the original confirmed outcome, and continue the existing authoritative
recovery path. Retrying the attachment with the original scope is allowed.
`cancelBeforeSubmission()` is only for a provably undispatched order; it never
cancels a venue order. A bound scope cannot be cancelled with this method.

On account reset/logout, stop the old watches, remove Swift observers, and call
`arca.resetPositionView(objectId:)` / `arca.resetPositionView(objectId)` before
creating new account watches. Old handles, tokens, and in-flight reads cannot
write into the replacement view. Kotlin `close()` and TypeScript `dispose()`
also retire views. Swift clients must explicitly reset account views.

## Reconciliation and limits

Receipts are cumulative order execution quantities, not canonical fill IDs.
Repeated evidence is idempotent; decreasing quantities and foreign identities
are ignored. Partial terminal execution uses its actual filled quantity: a
filled order or a cancelled IOC remainder does not by itself close a position.
Signed addition covers opens, increases, reductions, full closes and reversals.
Decimals support up to 38 significant and 38 fractional digits; unsupported or
unrepresentable values cannot become authoritative financial facts.

The view freezes each reserved market's baseline and overlays each tracked
operation once. It never adds all pending fills to an arbitrary incoming account
snapshot. Mixed accounting completion keeps **all** deltas until every active
scope in the account view is accounted. Then a coalesced fresh account read
reconciles them together. A generation ticket prevents a read begun before a
new scope/receipt from retiring it. Newer dated observations fence late old
snapshots, including snapshots that would resurrect a closed position.

The SDK's accounting path still refreshes authoritative account watches. The
view performs its own fresh read after coverage is established. Consumers do not
need an additional read for **display reconciliation**; keep any reads required
by their separate authoritative data stores until those stores consume the
account watch. A failed reconciliation read keeps pending/unknown display
provenance. A later structural observation or another `accounted()` call retries;
price ticks do not start a read loop. There is no additional timer polling.
Existing accounted() push, gap/reconnect, bounded backoff and timeout behavior
is retained. Keep the account-owned accounting wait/recovery alive after receipt
dismissal; long-lived or abandoned scopes can hold the view-wide barrier.

Unrelated observed same-market fills, delivery loss/reconnect, expired account
observations, and contradictory snapshots make the affected projections
unavailable. Delayed local fill events can be correlated after handle attachment;
recorded fills whose commit time precedes the baseline are already included.
Fresh accounted reconciliation restores authoritative display. Event correlation
buffers are bounded to 256 entries; overflow invalidates pending markets. Active
reservations and recent completed coverage are each bounded to 128.

There is no server position-inclusion cursor in today's receipt/account wire
contract. An **unobserved external same-market execution**, especially one whose
quantity happens to resemble partial accounting of known local fills, cannot be
identified by this SDK. Execution rows explicitly describe the result of known
local executions on the captured baseline, not guaranteed global current state.
The view refuses contradictions it can observe instead of guessing. A server
inclusion cursor would be required to eliminate this information limit.

## Validation

Deterministic regressions cover both account/receipt orderings, delayed account
state, opens/increases/reductions/full closes/reversals, exact decimal addition,
cumulative partial quantities, duplicate/foreign receipts, mixed accounting,
new scopes during reconciliation, external fills, fill-before-attachment,
contradictory snapshots, already-included fills, disconnect/reset, bounds and
pre-dispatch cancellation. Native mock-transport tests attach to a backend order
and verify that the shared display is updated before executionReceipt returns,
without issuing a mutation. No real trades are used.
