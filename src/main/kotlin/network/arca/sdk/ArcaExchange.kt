package network.arca.sdk

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.ensureActive
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.ActiveAssetData
import network.arca.sdk.models.AssetFeeEntry
import network.arca.sdk.models.Candle
import network.arca.sdk.models.CandleInterval
import network.arca.sdk.models.CandlesResponse
import network.arca.sdk.models.CreateArcaObjectResponse
import network.arca.sdk.models.ExchangeState
import network.arca.sdk.models.FeeTarget
import network.arca.sdk.models.FillListResponse
import network.arca.sdk.models.LeverageSetting
import network.arca.sdk.models.MarginMode
import network.arca.sdk.models.Market
import network.arca.sdk.models.MarketTickersResponse
import network.arca.sdk.models.MinOrderSize
import network.arca.sdk.models.Operation
import network.arca.sdk.models.OrderLimits
import network.arca.sdk.models.OrderListResponse
import network.arca.sdk.models.OrderOperationResponse
import network.arca.sdk.models.OrderSide
import network.arca.sdk.models.OrderSizeValidation
import network.arca.sdk.models.OrderStatus
import network.arca.sdk.models.OrderType
import network.arca.sdk.models.FundingHistoryResponse
import network.arca.sdk.models.OIHistoryResponse
import network.arca.sdk.models.PositionListResponse
import network.arca.sdk.models.PositionSide
import network.arca.sdk.models.SetMarginModeResponse
import network.arca.sdk.models.SimBookResponse
import network.arca.sdk.models.SimMetaResponse
import network.arca.sdk.models.SimMidsResponse
import network.arca.sdk.models.SimOrder
import network.arca.sdk.models.SimOrderWithFills
import network.arca.sdk.models.SimPosition
import network.arca.sdk.models.SparklinesResponse
import network.arca.sdk.models.TimeInForce
import network.arca.sdk.models.TpslType
import network.arca.sdk.models.TradeSummaryResponse
import network.arca.sdk.models.UpdateIsolatedMarginResponse
import network.arca.sdk.models.UpdateLeverageResponse
import java.util.UUID
import kotlin.coroutines.coroutineContext
import kotlin.math.ceil
import kotlin.math.pow
import kotlin.math.roundToLong

// MARK: - Exchange (Perps) Operations

/**
 * Ensure a Perps Exchange Arca object exists. Automatically sets `type=exchange`.
 *
 * Returns an [OperationHandle] — use `handle.settle()` to wait for full
 * settlement, or `handle.submitted()` for the HTTP response.
 *
 * @param ref Full Arca path (e.g. `/exchanges/hl1`).
 * @param venue Venue the exchange object trades against — `"hl-sim"` (default)
 *   provisions a simulated Hyperliquid account; `"hl"` provisions a live one.
 * @param operationPath Optional idempotency key.
 */
public fun Arca.ensurePerpsExchange(
    ref: String,
    venue: String = "hl-sim",
    operationPath: String? = null,
): OperationHandle<CreateArcaObjectResponse> =
    operationHandle {
        val metadataString = buildJsonObject { put("venue", venue) }.toString()
        val body = arcaJson.encodeToJsonElement(
            CreateExchangeRequest.serializer(),
            CreateExchangeRequest(
                realmId = realm,
                path = ref,
                type = "exchange",
                metadata = metadataString,
                operationPath = operationPath,
            ),
        )
        client.post<CreateArcaObjectResponse>("/objects", body = body)
    }

/** Get exchange account state (equity, margin, positions, orders). */
public suspend fun Arca.getExchangeState(objectId: String): ExchangeState =
    client.get("/objects/$objectId/exchange/state")

/**
 * Get active asset trading data: max trade sizes, margin, mark price, fee rate.
 *
 * @param objectId Exchange Arca object ID.
 * @param market Coin/asset in canonical format (e.g. `"hl:0:BTC"`, `"hl:1:SILVER"`).
 * @param applicationFeeTenthsBps Optional application fee in tenths of a basis point.
 * @param leverage Optional leverage override. When provided, the server uses this
 *   value instead of the stored leverage setting. When `null`, the server reads
 *   the leverage from the account's per-coin setting (defaulting to 1x).
 */
public suspend fun Arca.getActiveAssetData(
    objectId: String,
    market: String,
    applicationFeeTenthsBps: Int? = null,
    leverage: Int? = null,
): ActiveAssetData {
    val query = mutableMapOf("market" to market)
    if (applicationFeeTenthsBps != null && applicationFeeTenthsBps > 0) {
        query["applicationFeeTenthsBps"] = applicationFeeTenthsBps.toString()
    }
    if (leverage != null && leverage > 0) {
        query["leverage"] = leverage.toString()
    }
    return client.get("/objects/$objectId/exchange/active-asset-data", query = query)
}

/**
 * Get per-asset fee rates for an exchange object. Returns fully-composed
 * taker/maker rates accounting for volume tier, HIP-3 fee scale, platform fee,
 * and application fee.
 */
public suspend fun Arca.getAssetFees(
    objectId: String,
    applicationFeeTenthsBps: Int? = null,
): List<AssetFeeEntry> {
    val query = mutableMapOf<String, String>()
    if (applicationFeeTenthsBps != null && applicationFeeTenthsBps > 0) {
        query["applicationFeeTenthsBps"] = applicationFeeTenthsBps.toString()
    }
    return client.get("/objects/$objectId/exchange/asset-fees", query = query)
}

/** Update leverage for a coin on an exchange object. */
public suspend fun Arca.updateLeverage(
    objectId: String,
    market: String,
    leverage: Int,
): UpdateLeverageResponse {
    val body = arcaJson.encodeToJsonElement(
        UpdateLeverageRequest.serializer(),
        UpdateLeverageRequest(market = market, leverage = leverage),
    )
    return client.post("/objects/$objectId/exchange/leverage", body = body)
}

/**
 * Add or remove collateral from an isolated-margin position.
 *
 * A positive `amount` (decimal USD string) moves balance into the position,
 * lowering its liquidation price; a negative `amount` removes collateral,
 * raising it. Removal is rejected if it would drop the position below its
 * maintenance margin. Only valid on isolated positions.
 */
public suspend fun Arca.updateIsolatedMargin(
    objectId: String,
    market: String,
    amount: String,
): UpdateIsolatedMarginResponse {
    val body = arcaJson.encodeToJsonElement(
        UpdateIsolatedMarginRequest.serializer(),
        UpdateIsolatedMarginRequest(market = market, amount = amount),
    )
    return client.post("/objects/$objectId/exchange/isolated-margin", body = body)
}

/**
 * Switch an asset between cross and isolated margin for an exchange object.
 *
 * Rejected on isolated-only (HIP-3) markets and while an open position exists
 * for the asset — close the position first. Leverage is remembered per mode, so
 * switching restores the leverage last set for that mode.
 */
public suspend fun Arca.setMarginMode(
    objectId: String,
    market: String,
    marginMode: MarginMode,
): SetMarginModeResponse {
    val body = arcaJson.encodeToJsonElement(
        SetMarginModeRequest.serializer(),
        SetMarginModeRequest(market = market, marginMode = marginMode),
    )
    return client.post("/objects/$objectId/exchange/margin-mode", body = body)
}

/** Get leverage settings for a coin (or all coins) on an exchange object. */
public suspend fun Arca.getLeverage(
    objectId: String,
    market: String? = null,
): List<LeverageSetting> {
    val query = mutableMapOf<String, String>()
    if (market != null) query["market"] = market
    return if (market != null) {
        val single: LeverageSetting = client.get("/objects/$objectId/exchange/leverage", query = query)
        listOf(single)
    } else {
        client.get("/objects/$objectId/exchange/leverage", query = query)
    }
}

/**
 * Place an order on an exchange Arca object.
 *
 * Returns an [OrderHandle] with order lifecycle methods: `order.settle()`,
 * `order.filled()`, `order.fills()`, `order.onFill { }`, `order.cancel()`.
 *
 * @param path Operation path (idempotency key).
 * @param objectId Exchange Arca object ID.
 * @param market Coin/asset to trade.
 * @param side Order side ([OrderSide.BUY] or [OrderSide.SELL]).
 * @param orderType Order type ([OrderType.MARKET] or [OrderType.LIMIT]).
 * @param size Order size as decimal string.
 * @param price Limit price (required for limit orders).
 * @param leverage Optional leverage override. If omitted, uses the account's current per-coin leverage.
 * @param reduceOnly If true, only reduces an existing position.
 * @param timeInForce Time in force (default [TimeInForce.GTC]).
 * @param applicationFeeTenthsBps Application fee in tenths of a basis point.
 * @param feeTargets Fee routing targets.
 * @param isTrigger If true, this is a trigger (TP/SL) order.
 * @param triggerPx Trigger price — mark-price threshold to activate the order.
 * @param isMarket If true, execute as market order when triggered; if false, use price as limit.
 * @param tpsl Take profit ([TpslType.TAKE_PROFIT]) or stop loss ([TpslType.STOP_LOSS]).
 * @param sizeToMax Marks an *unsized* ("size to max") TP/SL — closes the **entire** position when triggered.
 * @param useMax When true, the server resolves max order size at execution time.
 * @param sizeTolerance Max allowed downward size adjustment as a fraction (0.01 = 1%).
 * @param maxSizeTolerance Deprecated — use [sizeTolerance].
 * @param ocoGroupId Links this order to the other legs of a TP/SL bracket (one-cancels-the-other).
 */
public fun Arca.placeOrder(
    path: String,
    objectId: String,
    market: String,
    side: OrderSide,
    orderType: OrderType,
    size: String,
    price: String? = null,
    leverage: Int? = null,
    reduceOnly: Boolean = false,
    timeInForce: TimeInForce = TimeInForce.GTC,
    applicationFeeTenthsBps: Int? = null,
    feeTargets: List<FeeTarget>? = null,
    isTrigger: Boolean? = null,
    triggerPx: String? = null,
    isMarket: Boolean? = null,
    tpsl: TpslType? = null,
    sizeToMax: Boolean? = null,
    useMax: Boolean? = null,
    sizeTolerance: Double? = null,
    maxSizeTolerance: Double? = null,
    isolated: Boolean? = null,
    ocoGroupId: String? = null,
): OrderHandle {
    val effectiveTolerance = sizeTolerance ?: maxSizeTolerance
    val inner = operationHandle<OrderOperationResponse> {
        val body = arcaJson.encodeToJsonElement(
            PlaceOrderRequest.serializer(),
            PlaceOrderRequest(
                realmId = realm,
                path = path,
                market = market,
                side = side.wire,
                orderType = orderType.wire,
                size = size,
                price = price,
                leverage = leverage,
                reduceOnly = reduceOnly,
                timeInForce = timeInForce.wire,
                applicationFeeTenthsBps = applicationFeeTenthsBps,
                feeTargets = feeTargets,
                isTrigger = isTrigger,
                triggerPx = triggerPx,
                isMarket = isMarket,
                tpsl = tpsl?.wire,
                sizeToMax = sizeToMax,
                useMax = useMax,
                sizeTolerance = effectiveTolerance,
                isolated = if (isolated == true) true else null,
                ocoGroupId = ocoGroupId,
            ),
        )
        client.post<OrderOperationResponse>("/objects/$objectId/exchange/orders", body = body)
    }
    return OrderHandle(
        scope = scope,
        inner = inner,
        objectId = objectId,
        placementPath = path,
        deps = makeOrderHandleDeps(),
    )
}

/** List orders for an exchange Arca object. */
public suspend fun Arca.listOrders(objectId: String, status: String? = null): List<SimOrder> {
    val query = mutableMapOf<String, String>()
    if (status != null) query["status"] = status
    val response: OrderListResponse = client.get("/objects/$objectId/exchange/orders", query = query)
    return response.orders
}

/** Get a specific order with its fills. */
public suspend fun Arca.getOrder(objectId: String, orderId: String): SimOrderWithFills =
    client.get("/objects/$objectId/exchange/orders/$orderId")

/**
 * Cancel an order on an exchange Arca object.
 *
 * Returns an [OperationHandle] — use `handle.settle()` to wait for full settlement.
 */
public fun Arca.cancelOrder(
    path: String,
    objectId: String,
    orderId: String,
): OperationHandle<OrderOperationResponse> =
    operationHandle {
        client.delete<OrderOperationResponse>(
            "/objects/$objectId/exchange/orders/$orderId",
            query = mapOf("realmId" to realm, "path" to path),
        )
    }

/**
 * Resize a resting order to a new total size.
 *
 * Only **sized** orders can be resized: resting limit orders and sized TP/SL
 * triggers. `newSize` is the new total size and must exceed the order's
 * already-filled quantity. `path` is the per-resize idempotency key.
 */
public fun Arca.modifyOrder(
    path: String,
    objectId: String,
    orderId: String,
    newSize: String,
): OperationHandle<OrderOperationResponse> =
    operationHandle {
        val body = arcaJson.encodeToJsonElement(
            ModifyOrderBody.serializer(),
            ModifyOrderBody(realmId = realm, path = path, newSize = newSize),
        )
        client.patch<OrderOperationResponse>("/objects/$objectId/exchange/orders/$orderId", body = body)
    }

/** List positions for an exchange Arca object. */
public suspend fun Arca.listPositions(objectId: String): List<SimPosition> {
    val response: PositionListResponse = client.get("/objects/$objectId/exchange/positions")
    return response.positions
}

/**
 * Close an open position (fully or partially) with `reduceOnly` enforced.
 *
 * Looks up the current position for the given coin, infers the closing side, and
 * places a market order sized to close the full position (or the specified
 * [size] for a partial close). Always sets `reduceOnly = true`.
 *
 * Threads the position's `leverage` into the order body and sets `isolated = true`
 * for HIP-3 (`onlyIsolated`) markets. Pass [isolated] / [leverage] to override.
 */
public fun Arca.closePosition(
    path: String,
    objectId: String,
    market: String,
    size: String? = null,
    timeInForce: TimeInForce = TimeInForce.IOC,
    applicationFeeTenthsBps: Int? = null,
    feeTargets: List<FeeTarget>? = null,
    isolated: Boolean? = null,
    leverage: Int? = null,
): OrderHandle {
    val self = this
    val inner = operationHandle<OrderOperationResponse> {
        val positions = self.listPositions(objectId)
        val position = positions.firstOrNull { it.market == market }
            ?: throw ArcaException.NotFound("POSITION_NOT_FOUND", "No open position for $market", null)
        val closingSide = if (position.side == PositionSide.LONG) OrderSide.SELL else OrderSide.BUY
        val closeSize = if (size != null) {
            val requestedVal = size.toDoubleOrNull() ?: 0.0
            val availableVal = position.size.toDoubleOrNull() ?: 0.0
            if (requestedVal > availableVal) position.size else size
        } else {
            position.size
        }
        val effectiveLeverage = leverage ?: position.leverage
        val effectiveIsolated = isolated ?: (runCatching { self.market(market) }.getOrNull()?.onlyIsolated == true)
        val body = arcaJson.encodeToJsonElement(
            PlaceOrderRequest.serializer(),
            PlaceOrderRequest(
                realmId = realm,
                path = path,
                market = market,
                side = closingSide.wire,
                orderType = OrderType.MARKET.wire,
                size = closeSize,
                price = null,
                leverage = effectiveLeverage,
                reduceOnly = true,
                timeInForce = timeInForce.wire,
                applicationFeeTenthsBps = applicationFeeTenthsBps,
                feeTargets = feeTargets,
                isTrigger = null,
                triggerPx = null,
                isMarket = null,
                tpsl = null,
                sizeToMax = null,
                useMax = null,
                sizeTolerance = null,
                isolated = if (effectiveIsolated) true else null,
            ),
        )
        client.post<OrderOperationResponse>("/objects/$objectId/exchange/orders", body = body)
    }
    return OrderHandle(
        scope = scope,
        inner = inner,
        objectId = objectId,
        placementPath = path,
        deps = makeOrderHandleDeps(),
    )
}

// MARK: - Position TP/SL (existing positions)

/**
 * Attach a stop-loss to the open position for [market].
 *
 * By default the trigger is placed *unsized* (`sizeToMax`, `reduceOnly`) — when
 * it fires it closes the **entire** live position and is cancelled when the
 * position closes. Pass a positive base-unit [size] for a **sized** partial close.
 * The closing side is inferred from the position; [leverage] / [isolated] are
 * auto-filled from the position and market meta.
 */
public fun Arca.setStopLoss(
    path: String,
    objectId: String,
    market: String,
    triggerPx: String,
    size: String? = null,
    isMarket: Boolean? = null,
    limitPrice: String? = null,
    replace: Boolean = true,
    leverage: Int? = null,
    isolated: Boolean? = null,
    timeInForce: TimeInForce = TimeInForce.GTC,
    applicationFeeTenthsBps: Int? = null,
    feeTargets: List<FeeTarget>? = null,
    ocoGroupId: String? = null,
): OrderHandle = setPositionTrigger(
    tpsl = TpslType.STOP_LOSS,
    path = path,
    objectId = objectId,
    market = market,
    triggerPx = triggerPx,
    size = size,
    isMarket = isMarket,
    limitPrice = limitPrice,
    replace = replace,
    leverage = leverage,
    isolated = isolated,
    timeInForce = timeInForce,
    applicationFeeTenthsBps = applicationFeeTenthsBps,
    feeTargets = feeTargets,
    ocoGroupId = ocoGroupId,
)

/** Attach a take-profit to the open position for [market]. */
public fun Arca.setTakeProfit(
    path: String,
    objectId: String,
    market: String,
    triggerPx: String,
    size: String? = null,
    isMarket: Boolean? = null,
    limitPrice: String? = null,
    replace: Boolean = true,
    leverage: Int? = null,
    isolated: Boolean? = null,
    timeInForce: TimeInForce = TimeInForce.GTC,
    applicationFeeTenthsBps: Int? = null,
    feeTargets: List<FeeTarget>? = null,
    ocoGroupId: String? = null,
): OrderHandle = setPositionTrigger(
    tpsl = TpslType.TAKE_PROFIT,
    path = path,
    objectId = objectId,
    market = market,
    triggerPx = triggerPx,
    size = size,
    isMarket = isMarket,
    limitPrice = limitPrice,
    replace = replace,
    leverage = leverage,
    isolated = isolated,
    timeInForce = timeInForce,
    applicationFeeTenthsBps = applicationFeeTenthsBps,
    feeTargets = feeTargets,
    ocoGroupId = ocoGroupId,
)

private fun Arca.setPositionTrigger(
    tpsl: TpslType,
    path: String,
    objectId: String,
    market: String,
    triggerPx: String,
    size: String?,
    isMarket: Boolean?,
    limitPrice: String?,
    replace: Boolean,
    leverage: Int?,
    isolated: Boolean?,
    timeInForce: TimeInForce,
    applicationFeeTenthsBps: Int?,
    feeTargets: List<FeeTarget>?,
    ocoGroupId: String?,
): OrderHandle {
    val inner = operationHandle<OrderOperationResponse> {
        val isMarketOrder = isMarket ?: true
        if (!isMarketOrder && (limitPrice ?: "").isEmpty()) {
            throw ArcaException.Validation(
                "trigger-limit orders require a limitPrice (omit isMarket for a market trigger)",
                null,
            )
        }
        val (side, effLeverage, effIsolated) = inferPositionCloseParams(
            objectId = objectId,
            market = market,
            leverageOverride = leverage,
            isolatedOverride = isolated,
        )
        if (replace) {
            val existing = findPositionTpslOrders(objectId, market, tpsl.wire)
            for (order in existing) {
                cancelOrder(
                    path = "$path/replace-${order.id.value}",
                    objectId = objectId,
                    orderId = order.id.value,
                ).submitted()
            }
        }
        // A non-empty `size` makes this a sized partial reduce-only trigger;
        // an empty size keeps the unsized sizeToMax close of the whole position.
        val sized = !(size ?: "").isEmpty()
        val body = arcaJson.encodeToJsonElement(
            PlaceOrderRequest.serializer(),
            PlaceOrderRequest(
                realmId = realm,
                path = path,
                market = market,
                side = side.wire,
                orderType = if (isMarketOrder) OrderType.MARKET.wire else OrderType.LIMIT.wire,
                size = if (sized) size!! else "0",
                price = if (isMarketOrder) null else limitPrice,
                leverage = effLeverage,
                reduceOnly = true,
                timeInForce = timeInForce.wire,
                applicationFeeTenthsBps = applicationFeeTenthsBps,
                feeTargets = feeTargets,
                isTrigger = true,
                triggerPx = triggerPx,
                isMarket = isMarketOrder,
                tpsl = tpsl.wire,
                sizeToMax = if (sized) null else true,
                useMax = null,
                sizeTolerance = null,
                isolated = if (effIsolated) true else null,
                ocoGroupId = ocoGroupId,
            ),
        )
        client.post<OrderOperationResponse>("/objects/$objectId/exchange/orders", body = body)
    }
    return OrderHandle(
        scope = scope,
        inner = inner,
        objectId = objectId,
        placementPath = path,
        deps = makeOrderHandleDeps(),
    )
}

/**
 * Attach a stop-loss and/or take-profit to an open position in one call. At
 * least one of [stopLossPx] / [takeProfitPx] must be provided. Legs are placed
 * sequentially (SL then TP); a placement failure surfaces immediately.
 */
public suspend fun Arca.setPositionTpsl(
    path: String,
    objectId: String,
    market: String,
    stopLossPx: String? = null,
    takeProfitPx: String? = null,
    stopLossSz: String? = null,
    takeProfitSz: String? = null,
    isMarket: Boolean? = null,
    replace: Boolean = true,
    applicationFeeTenthsBps: Int? = null,
    feeTargets: List<FeeTarget>? = null,
    ocoGroupId: String? = null,
): SetPositionTpslResult {
    if ((stopLossPx ?: "").isEmpty() && (takeProfitPx ?: "").isEmpty()) {
        throw ArcaException.Validation(
            "setPositionTpsl requires at least one of stopLossPx or takeProfitPx",
            null,
        )
    }
    // One opaque group id links both legs as a true one-cancels-the-other bracket
    // only when both legs are unsized; a caller who wants sized legs linked passes
    // an explicit ocoGroupId (auto-OCO on a sized leg is a footgun).
    val anySized = !(stopLossSz ?: "").isEmpty() || !(takeProfitSz ?: "").isEmpty()
    val groupId = ocoGroupId ?: if (anySized) null else generateOcoGroupId()
    var slHandle: OrderHandle? = null
    var tpHandle: OrderHandle? = null
    if (stopLossPx != null && stopLossPx.isNotEmpty()) {
        val handle = setStopLoss(
            path = "$path/sl",
            objectId = objectId,
            market = market,
            triggerPx = stopLossPx,
            size = stopLossSz,
            isMarket = isMarket,
            replace = replace,
            applicationFeeTenthsBps = applicationFeeTenthsBps,
            feeTargets = feeTargets,
            ocoGroupId = groupId,
        )
        handle.submitted()
        slHandle = handle
    }
    if (takeProfitPx != null && takeProfitPx.isNotEmpty()) {
        val handle = setTakeProfit(
            path = "$path/tp",
            objectId = objectId,
            market = market,
            triggerPx = takeProfitPx,
            size = takeProfitSz,
            isMarket = isMarket,
            replace = replace,
            applicationFeeTenthsBps = applicationFeeTenthsBps,
            feeTargets = feeTargets,
            ocoGroupId = groupId,
        )
        handle.submitted()
        tpHandle = handle
    }
    return SetPositionTpslResult(stopLoss = slHandle, takeProfit = tpHandle)
}

/**
 * Open a position and attach reduce-only TP/SL triggers as a linked `normalTpsl`
 * bracket — Hyperliquid parity. The entry and its triggers are submitted as a
 * single signed batch to one operation; one signature links the legs. The
 * trigger legs arm only when the entry fills, and the venue links them as
 * one-cancels-the-other so a fill on one cancels its sibling.
 *
 * `normalTpsl` is a **fixed-size parent-order bracket**: each TP/SL child
 * defaults to the entry's [size] (a `normalTpsl` child is a fixed-size leg of
 * the parent order, not a whole-position trigger). Pass [takeProfitSz] /
 * [stopLossSz] (positive base units) for a smaller partial-close child. For a
 * **whole-position** TP/SL that sizes to the entire live position (Hyperliquid
 * `positionTpsl`), use [setStopLoss] / [setTakeProfit] / [setPositionTpsl]
 * instead — a separate trigger-only model with no entry leg that is not
 * accepted here.
 *
 * Returns one [OrderHandle] per leg (`entry`, `takeProfit?`, `stopLoss?`), all
 * backed by the single bracket operation. At least one of [takeProfitPx] /
 * [stopLossPx] is required. Until the entry fills, a TP/SL child is not yet a
 * live venue order (no venue order id — addressable only by its cloid);
 * cancelling it before activation cancels the parent bracket.
 *
 * A single signature links the legs, but this is **not** a globally all-or-none
 * batch: Hyperliquid only guarantees whole-payload rejection for pre-validation
 * failures.
 */
public fun Arca.openWithBracket(
    path: String,
    objectId: String,
    market: String,
    side: OrderSide,
    size: String,
    orderType: OrderType = OrderType.MARKET,
    price: String? = null,
    leverage: Int? = null,
    isolated: Boolean = false,
    timeInForce: TimeInForce = TimeInForce.GTC,
    applicationFeeTenthsBps: Int? = null,
    takeProfitPx: String? = null,
    stopLossPx: String? = null,
    takeProfitSz: String? = null,
    stopLossSz: String? = null,
    triggersAreMarket: Boolean = true,
    grouping: String = "normalTpsl",
): OpenBracketResult {
    if ((takeProfitPx ?: "").isEmpty() && (stopLossPx ?: "").isEmpty()) {
        throw ArcaException.Validation("openWithBracket requires at least one of takeProfitPx or stopLossPx", null)
    }
    if (orderType == OrderType.LIMIT && (price ?: "").isEmpty()) {
        throw ArcaException.Validation("a LIMIT entry requires a price", null)
    }

    val tif = timeInForce.wire
    val closingSide = if (side == OrderSide.BUY) OrderSide.SELL else OrderSide.BUY
    val feeBps = applicationFeeTenthsBps
    val isolatedFlag: Boolean? = if (isolated) true else null

    // Build orders[] in request order: entry first, then the trigger legs.
    val orders = mutableListOf(
        BatchLegBody(
            market = market,
            side = side.wire,
            orderType = orderType.wire,
            size = size,
            price = if ((price ?: "").isEmpty()) null else price,
            leverage = leverage,
            timeInForce = tif,
            applicationFeeTenthsBps = feeBps,
            isolated = isolatedFlag,
        ),
    )
    fun trigger(tpsl: String, triggerPx: String, sz: String?): BatchLegBody {
        // A normalTpsl child is FIXED-SIZE: it defaults to the entry's `size`.
        // An explicit `sz` is a smaller partial-close child. We never send
        // sizeToMax here — that is the whole-position positionTpsl model, which
        // this endpoint rejects (use setStopLoss / setTakeProfit).
        val childSize = if ((sz ?: "").isEmpty()) size else sz!!
        return BatchLegBody(
            market = market,
            side = closingSide.wire,
            orderType = if (triggersAreMarket) OrderType.MARKET.wire else OrderType.LIMIT.wire,
            size = childSize,
            reduceOnly = true,
            timeInForce = tif,
            applicationFeeTenthsBps = feeBps,
            isTrigger = true,
            triggerPx = triggerPx,
            isMarket = triggersAreMarket,
            tpsl = tpsl,
            sizeToMax = null,
            isolated = isolatedFlag,
        )
    }
    if (takeProfitPx != null && takeProfitPx.isNotEmpty()) orders.add(trigger("tp", takeProfitPx, takeProfitSz))
    if (stopLossPx != null && stopLossPx.isNotEmpty()) orders.add(trigger("sl", stopLossPx, stopLossSz))

    val body = arcaJson.encodeToJsonElement(
        PlaceOrderBatchBody.serializer(),
        PlaceOrderBatchBody(realmId = realm, path = path, grouping = grouping, orders = orders),
    )

    // One shared batch call: all handles derive from this single Deferred, so the
    // HTTP request fires exactly once.
    val batchCall: Deferred<OrderOperationResponse> = scope.async {
        val resp: OrderOperationResponse = client.post("/objects/$objectId/exchange/orders/batch", body = body)
        throwIfOperationFailed(resp.operation)
        resp
    }

    val deps = makeOrderHandleDeps()
    // Each leg gets its own OrderHandle backed by the SAME batch operation. We
    // rewrite the operation's outcome to the leg's own order summary (which
    // carries `orderId`) so `.filled()` / `.cancel()` target the right order.
    // `tpsl == null` selects the entry (orders[0]).
    fun legHandle(tpsl: String?): OrderHandle {
        val inner = OperationHandle(
            scope = scope,
            submit = {
                val resp = batchCall.await()
                val outcome = selectLegOutcome(resp.operation.outcome, tpsl)
                resp.withOperation(resp.operation.withOutcome(outcome))
            },
            waitForSettlement = { operationId -> waitForSettlement(operationId) },
        )
        return OrderHandle(scope = scope, inner = inner, objectId = objectId, placementPath = path, deps = deps)
    }

    return OpenBracketResult(
        entry = legHandle(null),
        takeProfit = if ((takeProfitPx ?: "").isEmpty()) null else legHandle("tp"),
        stopLoss = if ((stopLossPx ?: "").isEmpty()) null else legHandle("sl"),
    )
}

/**
 * Cancel resting unsized (sizeToMax) trigger orders for [market]. [tpsl] narrows
 * the clear to a single leg; `null` clears both. Returns the orders that were
 * targeted for cancellation.
 */
public suspend fun Arca.clearPositionTpsl(
    path: String,
    objectId: String,
    market: String,
    tpsl: TpslType? = null,
): List<SimOrder> {
    val existing = findPositionTpslOrders(objectId, market, tpsl?.wire)
    for (order in existing) {
        cancelOrder(path = "$path/${order.id.value}", objectId = objectId, orderId = order.id.value).submitted()
    }
    return existing
}

/**
 * Look up the open position for [market] and derive the closing side, leverage,
 * and isolated flag needed by a reduce-only close/trigger order. Optional
 * overrides win over the inferred values.
 */
private suspend fun Arca.inferPositionCloseParams(
    objectId: String,
    market: String,
    leverageOverride: Int?,
    isolatedOverride: Boolean?,
): Triple<OrderSide, Int, Boolean> {
    val self = this
    val positions = listPositions(objectId)
    val position = positions.firstOrNull { it.market == market }
        ?: throw ArcaException.NotFound("POSITION_NOT_FOUND", "No open position for $market", null)
    val side = if (position.side == PositionSide.LONG) OrderSide.SELL else OrderSide.BUY
    val leverage = leverageOverride ?: position.leverage
    val isolated = when {
        isolatedOverride != null -> isolatedOverride
        else -> {
            val meta = runCatching { self.market(market) }.getOrNull()
            if (meta != null) {
                val modes = meta.marginModes
                if (!modes.isNullOrEmpty()) modes.size == 1 && modes.first() == "isolated" else meta.onlyIsolated
            } else {
                false
            }
        }
    }
    return Triple(side, leverage, isolated)
}

/**
 * Return resting unsized (sizeToMax) trigger orders for [market], optionally
 * narrowed to a single tp/sl leg.
 */
private suspend fun Arca.findPositionTpslOrders(
    objectId: String,
    market: String,
    tpsl: String?,
): List<SimOrder> {
    val orders = listOrders(objectId, OrderStatus.WAITING_FOR_TRIGGER.wire)
    return orders.filter {
        it.market == market && it.sizeToMax == true && (tpsl == null || it.tpsl == tpsl)
    }
}

private fun Arca.makeOrderHandleDeps(): OrderHandleDeps {
    val self = this
    return OrderHandleDeps(
        getOrder = { objId, orderId -> self.getOrder(objId, orderId) },
        fillEvents = { self.ws.fillEvents() },
        cancelOrder = { cancelPath, objId, orderId -> self.cancelOrder(cancelPath, objId, orderId) },
        modifyOrder = { modifyPath, objId, orderId, newSize -> self.modifyOrder(modifyPath, objId, orderId, newSize) },
        waitForSettlement = { operationId -> self.waitForSettlement(operationId) },
        listFills = { objId -> self.listFills(objId) },
    )
}

/**
 * List historical fills (trades) for an exchange Arca object. Returns paginated
 * fill data with P&L, fees, and resulting position state.
 *
 * @param objectId Exchange Arca object ID.
 * @param market Filter by market coin (e.g. `"hl:0:BTC"`).
 * @param startTime Filter fills on or after this timestamp (RFC 3339).
 * @param endTime Filter fills on or before this timestamp (RFC 3339).
 * @param limit Max fills to return (default 100, max 500).
 * @param cursor Cursor for pagination (createdAt of last fill).
 */
public suspend fun Arca.listFills(
    objectId: String,
    market: String? = null,
    startTime: String? = null,
    endTime: String? = null,
    limit: Int? = null,
    cursor: String? = null,
): FillListResponse {
    val query = mutableMapOf<String, String>()
    if (market != null) query["market"] = market
    if (startTime != null) query["startTime"] = startTime
    if (endTime != null) query["endTime"] = endTime
    if (limit != null) query["limit"] = limit.toString()
    if (cursor != null) query["cursor"] = cursor
    return client.get("/objects/$objectId/exchange/fills", query = query)
}

/**
 * Get per-market P&L aggregation for an exchange Arca object. Summarizes realized
 * P&L, total fees, trade count, and volume by market.
 */
public suspend fun Arca.tradeSummary(
    objectId: String,
    startTime: String? = null,
    endTime: String? = null,
): TradeSummaryResponse {
    val query = mutableMapOf<String, String>()
    if (startTime != null) query["startTime"] = startTime
    if (endTime != null) query["endTime"] = endTime
    return client.get("/objects/$objectId/exchange/trade-summary", query = query)
}

/** Get market metadata (supported assets). */
public suspend fun Arca.getMarketMeta(): SimMetaResponse =
    client.get("/exchange/market/meta")

/**
 * Look up a single market by its **exact canonical market ID** (e.g. `"hl:0:BTC"`,
 * `"hl:1:TSLA"`). This is an exact-id lookup — pass the `name` field of a [Market],
 * not a bare symbol like `"BTC"`. To go from a human symbol to its market(s), use
 * [resolveMarkets] / [resolveMarketOrThrow].
 *
 * Lazily fetches and caches market metadata on first call.
 *
 * @param id Canonical market ID (the `name` field on [Market]).
 * @return The matching [Market], or `null` if not found.
 */
public suspend fun Arca.market(id: String): Market? {
    val map = ensureMetaLoaded()
    return map[id]
}

/**
 * Resolve a human **symbol** (e.g. `"BTC"`, `"TSLA"`) to the market(s) that carry
 * it, returning an **array** because one symbol can map to many markets across
 * exchanges and HIP-3 dexes.
 *
 * Never fails silently: an empty list is an explicit "no market has this symbol".
 * Match is **exact and case-sensitive** on [Market.symbol]. Narrow ambiguous
 * symbols with the optional [exchange] / [dex] filters.
 */
public suspend fun Arca.resolveMarkets(
    symbol: String,
    exchange: String? = null,
    dex: String? = null,
): List<Market> {
    val map = ensureMetaLoaded()
    return map.values.filter { m ->
        m.symbol == symbol &&
            (exchange == null || m.exchange == exchange) &&
            (dex == null || m.dex == dex)
    }
}

/**
 * Resolve a human **symbol** to the single market that carries it, throwing when
 * the result is not exactly one. Use this when your code assumes a symbol is
 * unambiguous (often after narrowing with [exchange] / [dex]).
 *
 * @throws ArcaException.Validation when zero or more than one market matches.
 */
public suspend fun Arca.resolveMarketOrThrow(
    symbol: String,
    exchange: String? = null,
    dex: String? = null,
): Market {
    val matches = resolveMarkets(symbol, exchange = exchange, dex = dex)
    val filters = if (exchange != null || dex != null) {
        val parts = buildList {
            if (exchange != null) add("exchange: $exchange")
            if (dex != null) add("dex: $dex")
        }
        " (filters: ${parts.joinToString(", ")})"
    } else {
        ""
    }
    if (matches.isEmpty()) {
        throw ArcaException.Validation(
            "No market found for symbol \"$symbol\"$filters. Pass a canonical id to market(_:), " +
                "or list candidates with resolveMarkets(\"$symbol\").",
            null,
        )
    }
    if (matches.size > 1) {
        val names = matches.joinToString(", ") { it.name }
        throw ArcaException.Validation(
            "Symbol \"$symbol\"$filters is ambiguous — ${matches.size} markets match: $names. " +
                "Narrow with exchange / dex, or call market(_:) with the exact canonical id.",
            null,
        )
    }
    return matches[0]
}

/**
 * Eagerly fetch and cache market metadata. Call at app startup to avoid latency
 * on the first [market] call. Safe to call multiple times.
 */
public suspend fun Arca.preloadMarketMeta() {
    ensureMetaLoaded()
}

/** Force re-fetch market metadata, replacing the cache. */
public suspend fun Arca.refreshMarketMeta() {
    ensureMetaLoaded(forceRefresh = true)
}

/**
 * Venue-wide order limits (e.g. the $10 minimum notional). Static; no network
 * call. Reduce-only orders and unsized (`sizeToMax`) triggers are exempt so
 * dust positions can always be closed.
 */
public fun Arca.getOrderLimits(): OrderLimits = OrderLimits(minOrderNotionalUsd = 10.0)

/**
 * Compute the minimum valid order size for a resolved [market] at a given
 * [price].
 *
 * The venue enforces a minimum order **notional** (`size * price`), but a UI
 * that takes a size in base-asset units needs that expressed as a minimum
 * **size**. This converts the market's `minOrderNotionalUsd` into a size,
 * rounded **up** to the market's `szDecimals` precision so the result always
 * clears the floor. Reduce-only orders and unsized (`sizeToMax`) triggers are
 * exempt (any positive size down to one tick).
 */
public fun Arca.getMinOrderSize(
    market: Market,
    price: Double,
    reduceOnly: Boolean = false,
    isTrigger: Boolean = false,
    sizeToMax: Boolean = false,
): MinOrderSize = computeMinOrderSize(
    szDecimals = market.szDecimals,
    minNotionalUsd = market.minOrderNotionalUsd ?: getOrderLimits().minOrderNotionalUsd,
    price = price,
    reduceOnly = reduceOnly,
    isTrigger = isTrigger,
    sizeToMax = sizeToMax,
)

/**
 * Compute the minimum valid order size for a market id at a given price.
 * Fetches (and caches) market metadata via [market]; falls back to the
 * venue-wide [getOrderLimits] default when the market is unknown or carries no
 * `minOrderNotionalUsd`.
 */
public suspend fun Arca.getMinOrderSize(
    marketId: String,
    price: Double,
    reduceOnly: Boolean = false,
    isTrigger: Boolean = false,
    sizeToMax: Boolean = false,
): MinOrderSize {
    val m = market(marketId)
    return computeMinOrderSize(
        szDecimals = m?.szDecimals ?: 5,
        minNotionalUsd = m?.minOrderNotionalUsd ?: getOrderLimits().minOrderNotionalUsd,
        price = price,
        reduceOnly = reduceOnly,
        isTrigger = isTrigger,
        sizeToMax = sizeToMax,
    )
}

/**
 * Validate an order size against a resolved [market]'s minimum before placing
 * an order. Advisory only — the server (sim-exchange and Hyperliquid) remains
 * the authoritative enforcement point; use this to gate a UI.
 */
public fun Arca.validateOrderSize(
    market: Market,
    price: Double,
    size: Double,
    reduceOnly: Boolean = false,
    isTrigger: Boolean = false,
    sizeToMax: Boolean = false,
): OrderSizeValidation {
    val min = getMinOrderSize(market, price, reduceOnly, isTrigger, sizeToMax)
    return checkOrderSize(min, price, size, reduceOnly, isTrigger, sizeToMax)
}

/** Validate an order size for a market id. Fetches market metadata as needed. */
public suspend fun Arca.validateOrderSize(
    marketId: String,
    price: Double,
    size: Double,
    reduceOnly: Boolean = false,
    isTrigger: Boolean = false,
    sizeToMax: Boolean = false,
): OrderSizeValidation {
    val min = getMinOrderSize(marketId, price, reduceOnly, isTrigger, sizeToMax)
    return checkOrderSize(min, price, size, reduceOnly, isTrigger, sizeToMax)
}

private fun computeMinOrderSize(
    szDecimals: Int,
    minNotionalUsd: Double,
    price: Double,
    reduceOnly: Boolean,
    isTrigger: Boolean,
    sizeToMax: Boolean,
): MinOrderSize {
    val factor = 10.0.pow(szDecimals)
    val tick = 1.0 / factor

    // Reduce-only and unsized trigger orders are exempt from the notional
    // minimum — any positive size down to one tick is allowed.
    if (reduceOnly || (isTrigger && sizeToMax)) {
        return MinOrderSize(formatSizeToDecimals(tick, szDecimals), 0.0)
    }

    if (!price.isFinite() || price <= 0) {
        return MinOrderSize(formatSizeToDecimals(tick, szDecimals), minNotionalUsd)
    }

    // Round up to szDecimals precision. Subtract a tiny epsilon on the scaled
    // value so floating-point noise on an exact boundary (e.g. 10 / 100000)
    // doesn't overshoot by a full tick.
    var minSizeNum = ceil(minNotionalUsd / price * factor - 1e-6) / factor
    if (minSizeNum < tick) minSizeNum = tick
    return MinOrderSize(formatSizeToDecimals(minSizeNum, szDecimals), minNotionalUsd)
}

private fun checkOrderSize(
    min: MinOrderSize,
    price: Double,
    size: Double,
    reduceOnly: Boolean,
    isTrigger: Boolean,
    sizeToMax: Boolean,
): OrderSizeValidation {
    if (!size.isFinite() || size <= 0) {
        return OrderSizeValidation(ok = false, minSize = min.minSize, minNotionalUsd = min.minNotionalUsd, reason = "Order size must be a positive number.")
    }

    // Exempt orders (reduce-only / unsized trigger) only need a positive size.
    if (reduceOnly || (isTrigger && sizeToMax)) {
        return OrderSizeValidation(ok = true, minSize = min.minSize, minNotionalUsd = min.minNotionalUsd)
    }

    val minSizeNum = min.minSize.toDoubleOrNull() ?: 0.0
    if (size < minSizeNum) {
        val notional = if (price.isFinite()) size * price else 0.0
        val reason = "Order notional \$${"%.2f".format(notional)} is below venue minimum of " +
            "\$${formatNotionalUsd(min.minNotionalUsd)}. Minimum size is ${min.minSize}."
        return OrderSizeValidation(ok = false, minSize = min.minSize, minNotionalUsd = min.minNotionalUsd, reason = reason)
    }

    return OrderSizeValidation(ok = true, minSize = min.minSize, minNotionalUsd = min.minNotionalUsd)
}

/**
 * Format a size to at most [decimals] fractional digits, stripping trailing
 * zeros, for use as a canonical decimal string (e.g. "0.0001", "3.34", "10").
 */
private fun formatSizeToDecimals(value: Double, decimals: Int): String {
    if (decimals <= 0) return value.roundToLong().toString()
    var s = "%.${decimals}f".format(value)
    if (s.contains(".")) {
        s = s.trimEnd('0').trimEnd('.')
    }
    return if (s.isEmpty()) "0" else s
}

/** Format a USD notional dropping a trailing `.0` (e.g. 10.0 -> "10", 10.5 -> "10.5"). */
private fun formatNotionalUsd(value: Double): String =
    if (value == value.toLong().toDouble()) value.toLong().toString() else value.toString()

/** Get current mid prices for all assets. */
public suspend fun Arca.getMarketMids(): SimMidsResponse =
    client.get("/exchange/market/mids")

/** Get 24h ticker data for all assets (volume, price change, funding, delisted status). */
public suspend fun Arca.getMarketTickers(): MarketTickersResponse =
    client.get("/exchange/market/tickers")

/** Get L2 order book for a specific coin. */
public suspend fun Arca.getOrderBook(market: String): SimBookResponse =
    client.get("/exchange/market/book/$market")

/**
 * Get OHLCV candle data for a specific coin.
 *
 * When `candleCdnBaseUrl` is configured and the interval is not `15s`, fetches
 * from CDN chunks for historical data with REST API fallback.
 *
 * @param market Canonical coin ID (e.g. `hl:0:BTC`, `hl:0:ETH`).
 * @param interval Candle interval (e.g. [CandleInterval.ONE_MINUTE]).
 * @param startTime Optional start time in epoch milliseconds.
 * @param endTime Optional end time in epoch milliseconds.
 * @param skipBackfill When true, the server returns only cached data without
 *   waiting for synchronous Hyperliquid backfill. Use for fast initial renders.
 */
public suspend fun Arca.getCandles(
    market: String,
    interval: CandleInterval,
    startTime: Long? = null,
    endTime: Long? = null,
    skipBackfill: Boolean = false,
): CandlesResponse {
    val nowMs = System.currentTimeMillis()
    val dur = interval.milliseconds
    val effectiveEnd = endTime ?: (nowMs / dur * dur)
    val key = buildCacheKey(
        "candles",
        mapOf(
            "market" to market,
            "interval" to interval.wire,
            "startTime" to startTime?.toString(),
            "endTime" to effectiveEnd.toString(),
        ),
    )
    historyCache.get<CandlesResponse>(key)?.let { return it }

    coroutineContext.ensureActive()

    if (candleCdnBaseUrl != null && interval != CandleInterval.FIFTEEN_SECONDS && startTime != null) {
        val candles = CandleCdn.fetchCandlesFromCdn(
            baseUrl = candleCdnBaseUrl,
            market = market,
            interval = interval,
            startMs = startTime,
            endMs = effectiveEnd,
            httpClient = httpClient,
            logger = log,
            apiFallback = { s, e ->
                val q = mutableMapOf("interval" to interval.wire)
                q["startTime"] = s.toString()
                q["endTime"] = e.toString()
                if (skipBackfill) q["skipBackfill"] = "true"
                val resp: CandlesResponse = client.get("/exchange/market/candles/$market", query = q)
                resp.candles
            },
        )
        val result = CandlesResponse(market = market, interval = interval.wire, candles = candles)
        if (candles.isNotEmpty()) {
            historyCache.set(key, result)
        }
        return result
    }

    val query = mutableMapOf("interval" to interval.wire)
    if (startTime != null) query["startTime"] = startTime.toString()
    if (endTime != null) query["endTime"] = endTime.toString()
    if (skipBackfill) query["skipBackfill"] = "true"
    val result: CandlesResponse = client.get("/exchange/market/candles/$market", query = query)
    historyCache.set(key, result)
    return result
}

/**
 * Get open-interest + 24h-notional-volume history for a market.
 *
 * Each bar tracks open interest (OHLC over the bucket, base-asset units) plus
 * the rolling 24h notional volume ([OIBar.ntlVlm], USD) and last mark price
 * ([OIBar.mark]) at the bucket close; USD OI ≈ `oiClose * mark`. Deep history
 * (~1 year) is seeded from a one-time 0xArchive backfill ([OIBar.s] == "0xa").
 *
 * @param market Canonical coin ID (e.g. `hl:0:BTC`).
 * @param interval OI interval (e.g. [CandleInterval.ONE_MINUTE]).
 * @param startTime Optional start time in epoch milliseconds.
 * @param endTime Optional end time in epoch milliseconds.
 */
public suspend fun Arca.getOIHistory(
    market: String,
    interval: CandleInterval,
    startTime: Long? = null,
    endTime: Long? = null,
): OIHistoryResponse {
    val key = buildCacheKey(
        "oiHistory",
        mapOf(
            "market" to market,
            "interval" to interval.wire,
            "startTime" to startTime?.toString(),
            "endTime" to endTime?.toString(),
        ),
    )
    historyCache.get<OIHistoryResponse>(key)?.let { return it }

    coroutineContext.ensureActive()

    val query = mutableMapOf("interval" to interval.wire)
    if (startTime != null) query["startTime"] = startTime.toString()
    if (endTime != null) query["endTime"] = endTime.toString()
    val result: OIHistoryResponse = client.get("/exchange/market/oi/$market", query = query)
    if (result.bars.isNotEmpty()) {
        historyCache.set(key, result)
    }
    return result
}

/**
 * Get market-wide SETTLED funding-rate history for a market.
 *
 * Returns the venue's real settlement series — each observation carries the
 * settlement time ([FundingObservation.t], Unix ms), the settled
 * [FundingObservation.fundingRate], and the [FundingObservation.premium] — in
 * chronological order. This is distinct from the account-scoped funding
 * *payments* (`watchFunding`); these are the market-wide rates that drive those
 * payments. [market] must be a canonical market id (e.g. `hl:0:BTC`,
 * `hl:1:TSLA`); HIP-3 markets are supported where history exists. Funding is an
 * event series, so there is no interval. The default window is the trailing 7
 * days; the documented maximum window is 30 days. Values are settled rates,
 * never predicted — read the ticker's funding + nextFundingTime for the
 * current/predicted rate.
 *
 * @param market Canonical market ID (e.g. `hl:0:BTC`).
 * @param startTime Optional start time in epoch milliseconds.
 * @param endTime Optional end time in epoch milliseconds.
 */
public suspend fun Arca.getFundingHistory(
    market: String,
    startTime: Long? = null,
    endTime: Long? = null,
): FundingHistoryResponse {
    val key = buildCacheKey(
        "fundingHistory",
        mapOf(
            "market" to market,
            "startTime" to startTime?.toString(),
            "endTime" to endTime?.toString(),
        ),
    )
    historyCache.get<FundingHistoryResponse>(key)?.let { return it }

    coroutineContext.ensureActive()

    val query = mutableMapOf<String, String>()
    if (startTime != null) query["startTime"] = startTime.toString()
    if (endTime != null) query["endTime"] = endTime.toString()
    val result: FundingHistoryResponse = client.get("/exchange/market/funding/$market", query = query)
    if (result.funding.isNotEmpty()) {
        historyCache.set(key, result)
    }
    return result
}

/**
 * Get sparkline close-price arrays for all tracked coins in a single request.
 * Returns a map of coin name to recent hourly close prices. Sparkline data is
 * pre-computed every ~5 minutes; for real-time prices use `watchPrices`.
 *
 * The [interval] / [points] parameters are accepted for backward compatibility
 * but ignored — sparklines always return 24 hourly close prices.
 */
@Suppress("UNUSED_PARAMETER")
public suspend fun Arca.getSparklines(
    interval: CandleInterval = CandleInterval.ONE_HOUR,
    points: Int = 24,
): SparklinesResponse =
    client.get("/exchange/market/sparklines")

// MARK: - Bracket / OCO helpers

/**
 * Pick one leg's order summary out of a bracket operation's outcome and return it
 * as a JSON string (carrying that leg's `orderId`). `tpsl == null` selects the
 * entry (`orders[0]`). Falls back to the whole outcome when the shape is
 * unexpected so handle resolution still has something to read.
 */
private fun selectLegOutcome(outcome: String?, tpsl: String?): String? {
    if (outcome == null) return null
    return runCatching {
        val obj = arcaJson.parseToJsonElement(outcome).jsonObject
        val legs = obj["orders"]?.jsonArray
        if (legs.isNullOrEmpty()) {
            outcome
        } else {
            val chosen = if (tpsl != null) {
                legs.firstOrNull { it.jsonObject["tpsl"]?.jsonPrimitive?.contentOrNull == tpsl }
            } else {
                legs.firstOrNull()
            }
            chosen?.toString() ?: outcome
        }
    }.getOrDefault(outcome)
}

/**
 * Mint a fresh opaque id that links the legs of a TP/SL bracket as
 * one-cancels-the-other. Advisory and only needs to be unique within a single
 * account's live order set, so a random UUID is sufficient.
 */
internal fun generateOcoGroupId(): String = "oco_${UUID.randomUUID()}"

private fun Operation.withOutcome(newOutcome: String?): Operation = copy(outcome = newOutcome)

// MARK: - Position TP/SL result types

/**
 * Handles for the legs placed by [Arca.setPositionTpsl]. A leg is `null` when its
 * trigger price was not provided.
 */
public data class SetPositionTpslResult(
    public val stopLoss: OrderHandle?,
    public val takeProfit: OrderHandle?,
)

/**
 * Handles for the legs placed by [Arca.openWithBracket]. All three handles are
 * backed by the **single** bracket operation; `takeProfit` / `stopLoss` are
 * `null` when their trigger price was not provided.
 */
public data class OpenBracketResult(
    public val entry: OrderHandle,
    public val takeProfit: OrderHandle?,
    public val stopLoss: OrderHandle?,
)

// MARK: - Request Bodies

@Serializable
private data class CreateExchangeRequest(
    val realmId: String,
    val path: String,
    val type: String,
    val metadata: String? = null,
    val operationPath: String? = null,
)

@Serializable
private data class UpdateLeverageRequest(
    val market: String,
    val leverage: Int,
)

@Serializable
private data class UpdateIsolatedMarginRequest(
    val market: String,
    val amount: String,
)

@Serializable
private data class SetMarginModeRequest(
    val market: String,
    val marginMode: MarginMode,
)

@Serializable
private data class PlaceOrderRequest(
    val realmId: String,
    val path: String,
    val market: String,
    val side: String,
    val orderType: String,
    val size: String,
    val price: String? = null,
    val leverage: Int? = null,
    val reduceOnly: Boolean,
    val timeInForce: String,
    val applicationFeeTenthsBps: Int? = null,
    val feeTargets: List<FeeTarget>? = null,
    val isTrigger: Boolean? = null,
    val triggerPx: String? = null,
    val isMarket: Boolean? = null,
    val tpsl: String? = null,
    val sizeToMax: Boolean? = null,
    val useMax: Boolean? = null,
    val sizeTolerance: Double? = null,
    val isolated: Boolean? = null,
    val ocoGroupId: String? = null,
)

@Serializable
private data class ModifyOrderBody(
    val realmId: String,
    val path: String,
    val newSize: String,
)

/**
 * One leg of a [PlaceOrderBatchBody]. Optional fields are omitted from the JSON
 * when `null`, so the entry leg and the unsized trigger legs share one shape.
 * `ocoGroupId` is deliberately absent — the venue server-stamps the shared group
 * id on the trigger legs.
 */
@Serializable
private data class BatchLegBody(
    val market: String,
    val side: String,
    val orderType: String? = null,
    val size: String,
    val price: String? = null,
    val leverage: Int? = null,
    val reduceOnly: Boolean? = null,
    val timeInForce: String? = null,
    val applicationFeeTenthsBps: Int? = null,
    val isTrigger: Boolean? = null,
    val triggerPx: String? = null,
    val isMarket: Boolean? = null,
    val tpsl: String? = null,
    val sizeToMax: Boolean? = null,
    val isolated: Boolean? = null,
)

/**
 * Request body for the atomic batch endpoint. One signed `eip712.OrderBatchAction`
 * is built server-side over `orders[] + grouping`.
 */
@Serializable
private data class PlaceOrderBatchBody(
    val realmId: String,
    val path: String,
    val grouping: String,
    val orders: List<BatchLegBody>,
)
