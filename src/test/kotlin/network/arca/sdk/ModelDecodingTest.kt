package network.arca.sdk

import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.int
import network.arca.sdk.internal.ApiResponse
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.AssetBreakdown
import network.arca.sdk.models.AssetCategory
import network.arca.sdk.models.ActiveAssetData
import network.arca.sdk.models.ArcaBalance
import network.arca.sdk.models.ArcaObject
import network.arca.sdk.models.ArcaObjectBrowseResponse
import network.arca.sdk.models.ArcaObjectDetailResponse
import network.arca.sdk.models.ArcaObjectType
import network.arca.sdk.models.ArcaPositionCurrent
import network.arca.sdk.models.CreateWatchResponse
import network.arca.sdk.models.DeltaType
import network.arca.sdk.models.EventType
import network.arca.sdk.models.ExchangeState
import network.arca.sdk.models.ExplorerSummary
import network.arca.sdk.models.Fill
import network.arca.sdk.models.FundAccountResponse
import network.arca.sdk.models.FundingHistoryResponse
import network.arca.sdk.models.LeverageSetting
import network.arca.sdk.models.LeverageType
import network.arca.sdk.models.MarginMode
import network.arca.sdk.models.MarketTicker
import network.arca.sdk.models.ObjectValuation
import network.arca.sdk.models.Operation
import network.arca.sdk.models.OperationDetailResponse
import network.arca.sdk.models.OperationListResponse
import network.arca.sdk.models.OperationState
import network.arca.sdk.models.OperationType
import network.arca.sdk.models.OrderListResponse
import network.arca.sdk.models.OrderSide
import network.arca.sdk.models.OrderStatus
import network.arca.sdk.models.OrderType
import network.arca.sdk.models.PathAggregation
import network.arca.sdk.models.PnlAnchor
import network.arca.sdk.models.PnlPoint
import network.arca.sdk.models.PnlResponse
import network.arca.sdk.models.PositionListResponse
import network.arca.sdk.models.PositionSide
import network.arca.sdk.models.PricingMode
import network.arca.sdk.models.Realm
import network.arca.sdk.models.RealmEvent
import network.arca.sdk.models.RealmType
import network.arca.sdk.models.SetMarginModeResponse
import network.arca.sdk.models.SimAccount
import network.arca.sdk.models.SimBookResponse
import network.arca.sdk.models.SimFill
import network.arca.sdk.models.SimMarginSummary
import network.arca.sdk.models.SimPosition
import network.arca.sdk.models.SnapshotBalancesResponse
import network.arca.sdk.models.SparklinesResponse
import network.arca.sdk.models.StateDelta
import network.arca.sdk.models.TimeInForce
import network.arca.sdk.models.TypedEvent
import network.arca.sdk.models.UpdateIsolatedMarginResponse
import network.arca.sdk.models.applyEquityAnchor
import network.arca.sdk.models.revalued
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.math.BigDecimal

@Serializable
private data class LeverageBody(val market: String, val leverage: Int)

class ModelDecodingTest {

    private fun assertDecimalEquals(expected: String, actual: String?) {
        assertNotNull(actual)
        assertEquals(0, BigDecimal(expected).compareTo(BigDecimal(actual)), "expected $expected == $actual")
    }

    // MARK: - TypedID

    @Test
    fun typedIdDecoding() {
        val id = arcaJson.decodeFromString<ObjectId>("\"obj_01h2xcejqtf2nbrexx3vqjhp41\"")
        assertEquals("obj_01h2xcejqtf2nbrexx3vqjhp41", id.value)
    }

    @Test
    fun typedIdEncoding() {
        val json = arcaJson.encodeToString(ObjectId("obj_test123"))
        assertEquals("\"obj_test123\"", json)
    }

    @Test
    fun typedIdEquality() {
        assertEquals(ObjectId("obj_abc"), ObjectId("obj_abc"))
        assertNotEquals(ObjectId("obj_abc"), ObjectId("obj_def"))
    }

    @Test
    fun differentTagTypesHoldValues() {
        assertEquals("obj_abc", ObjectId("obj_abc").value)
        assertEquals("op_abc", OperationId("op_abc").value)
    }

    // MARK: - ArcaObject

    @Test
    fun arcaObjectDecoding() {
        val json = """
            {
                "id": "obj_01h2xcejqtf2nbrexx3vqjhp41",
                "realmId": "rlm_01h2xcejqtf2nbrexx3vqjhp42",
                "path": "/wallets/main",
                "type": "denominated",
                "denomination": "USD",
                "status": "active",
                "metadata": null,
                "deletedAt": null,
                "systemOwned": false,
                "createdAt": "2026-03-07T10:00:00.000000Z",
                "updatedAt": "2026-03-07T10:00:00.000000Z"
            }
        """.trimIndent()
        val obj = arcaJson.decodeFromString<ArcaObject>(json)
        assertEquals("obj_01h2xcejqtf2nbrexx3vqjhp41", obj.id.value)
        assertEquals("/wallets/main", obj.path)
        assertEquals(ArcaObjectType.DENOMINATED, obj.type)
        assertEquals("USD", obj.denomination)
        assertEquals(network.arca.sdk.models.ArcaObjectStatus.ACTIVE, obj.status)
        assertFalse(obj.systemOwned)
        assertNull(obj.metadata)
        assertNull(obj.deletedAt)
    }

    @Test
    fun arcaObjectTypeValues() {
        for (typeStr in listOf("denominated", "exchange", "deposit", "withdrawal", "escrow")) {
            val decoded = arcaJson.decodeFromString<ArcaObjectType>("\"$typeStr\"")
            assertEquals(typeStr, decoded.value)
        }
    }

    @Test
    fun arcaObjectTypeInfoDecoding() {
        assertEquals(ArcaObjectType.INFO, arcaJson.decodeFromString<ArcaObjectType>("\"info\""))
    }

    @Test
    fun arcaObjectTypeUnknownDecoding() {
        assertEquals(ArcaObjectType("future_type"), arcaJson.decodeFromString<ArcaObjectType>("\"future_type\""))
    }

    @Test
    fun arcaObjectTypeRoundTrips() {
        for (typeStr in listOf("denominated", "exchange", "deposit", "withdrawal", "escrow", "info")) {
            val decoded = arcaJson.decodeFromString<ArcaObjectType>("\"$typeStr\"")
            assertEquals("\"$typeStr\"", arcaJson.encodeToString(decoded))
        }
    }

    @Test
    fun arcaObjectWithInfoType() {
        val json = """
            {
                "id": "obj_info01", "realmId": "rlm_01def", "path": "/.info", "type": "info",
                "denomination": null, "status": "active", "metadata": null, "deletedAt": null,
                "systemOwned": true, "createdAt": "2026-03-28T10:00:00.000000Z", "updatedAt": "2026-03-28T10:00:00.000000Z"
            }
        """.trimIndent()
        val obj = arcaJson.decodeFromString<ArcaObject>(json)
        assertEquals(ArcaObjectType.INFO, obj.type)
        assertEquals("/.info", obj.path)
        assertTrue(obj.systemOwned)
    }

    // MARK: - Operation

    @Test
    fun operationDecoding() {
        val json = """
            {
                "id": "op_01abc", "realmId": "rlm_01def", "path": "/op/transfer/1", "type": "transfer",
                "state": "completed", "sourceArcaPath": "/wallets/a", "targetArcaPath": "/wallets/b",
                "input": null, "outcome": "{\"newBalance\":\"500\"}", "actorType": "BUILDER",
                "actorId": "usr_01xyz", "tokenJti": null,
                "createdAt": "2026-03-07T10:00:00.000000Z", "updatedAt": "2026-03-07T10:01:00.000000Z"
            }
        """.trimIndent()
        val op = arcaJson.decodeFromString<Operation>(json)
        assertEquals(OperationType.TRANSFER, op.type)
        assertEquals(OperationState.COMPLETED, op.state)
        assertTrue(op.state.isTerminal)
        assertEquals("/wallets/a", op.sourceArcaPath)
        assertEquals("/wallets/b", op.targetArcaPath)
    }

    @Test
    fun operationDecodingWithFailureMessage() {
        val json = """
            {
                "id": "op_01abc", "realmId": "rlm_01def", "path": "/op/transfer/1", "type": "transfer",
                "state": "failed", "sourceArcaPath": "/wallets/a", "targetArcaPath": "/wallets/b",
                "input": null, "outcome": "{\"reason\":\"CHAIN_SEND_FAILED\"}",
                "failureMessage": "CHAIN_SEND_FAILED", "actorType": "BUILDER", "actorId": "usr_01xyz", "tokenJti": null,
                "createdAt": "2026-03-07T10:00:00.000000Z", "updatedAt": "2026-03-07T10:01:00.000000Z"
            }
        """.trimIndent()
        val op = arcaJson.decodeFromString<Operation>(json)
        assertEquals(OperationState.FAILED, op.state)
        assertEquals("CHAIN_SEND_FAILED", op.failureMessage)
    }

    @Test
    fun operationDecodingWithoutFailureMessage() {
        val json = """
            {
                "id": "op_01abc", "realmId": "rlm_01def", "path": "/op/transfer/1", "type": "transfer",
                "state": "completed", "sourceArcaPath": "/wallets/a", "targetArcaPath": "/wallets/b",
                "input": null, "outcome": null, "actorType": "BUILDER", "actorId": "usr_01xyz", "tokenJti": null,
                "createdAt": "2026-03-07T10:00:00.000000Z", "updatedAt": "2026-03-07T10:01:00.000000Z"
            }
        """.trimIndent()
        val op = arcaJson.decodeFromString<Operation>(json)
        assertEquals(OperationState.COMPLETED, op.state)
        assertNull(op.failureMessage)
    }

    @Test
    fun operationTypeIncludesFillFundingAdjustment() {
        assertEquals(OperationType.FILL, arcaJson.decodeFromString<OperationType>("\"fill\""))
        assertEquals(OperationType.FUNDING, arcaJson.decodeFromString<OperationType>("\"funding\""))
        assertEquals(OperationType.ADJUSTMENT, arcaJson.decodeFromString<OperationType>("\"adjustment\""))
    }

    @Test
    fun operationTypeIncludesVenueClose() {
        val decoded = arcaJson.decodeFromString<OperationType>("\"venue_close\"")
        assertEquals(OperationType.VENUE_CLOSE, decoded)
        assertEquals("venue_close", decoded.value)
    }

    @Test
    fun operationTypePreservesUnknownFutureValue() {
        val decoded = arcaJson.decodeFromString<OperationType>("\"future_operation\"")
        assertEquals(OperationType("future_operation"), decoded)
        assertEquals("future_operation", decoded.value)
    }

    @Test
    fun venueCloseOperationListDecoding() {
        val json = """
            {
                "operations": [
                    {
                        "id": "op_close01", "realmId": "rlm_01def",
                        "path": "/op/venue_close/exchanges/main/op_close01", "type": "venue_close",
                        "state": "completed", "sourceArcaPath": null, "targetArcaPath": "/exchanges/main",
                        "input": null, "outcome": null, "actorType": "system", "actorId": "sim-exchange", "tokenJti": null,
                        "createdAt": "2026-04-28T05:11:47.000000Z", "updatedAt": "2026-04-28T05:11:47.000000Z"
                    }
                ],
                "total": 1
            }
        """.trimIndent()
        val response = arcaJson.decodeFromString<OperationListResponse>(json)
        assertEquals(1, response.operations.count())
        assertEquals(OperationType.VENUE_CLOSE, response.operations[0].type)
    }

    @Test
    fun realmEventDecodesOperationWithUnknownFutureType() {
        val json = """
            {
                "realmId": "rlm_01def", "type": "operation.updated", "entityId": "op_future01",
                "operation": {
                    "id": "op_future01", "realmId": "rlm_01def", "path": "/op/future/1", "type": "future_operation",
                    "state": "completed", "sourceArcaPath": null, "targetArcaPath": "/exchanges/main",
                    "input": null, "outcome": null, "actorType": "system", "actorId": "sim-exchange", "tokenJti": null,
                    "createdAt": "2026-04-28T05:11:47.000000Z", "updatedAt": "2026-04-28T05:11:47.000000Z"
                }
            }
        """.trimIndent()
        val event = arcaJson.decodeFromString<RealmEvent>(json)
        assertEquals(OperationType("future_operation"), event.operation?.type)
        assertEquals("future_operation", event.operation?.type?.value)
    }

    @Test
    fun operationDecodingWithParsedOutcomeContainingArray() {
        val json = """
            {
                "id": "op_recon01", "realmId": "rlm_01def",
                "path": "/op/fill/exchanges/main/ord_01/op_recon01", "type": "fill",
                "state": "completed", "sourceArcaPath": null, "targetArcaPath": "/exchanges/main",
                "input": null, "outcome": null,
                "parsedOutcome": {
                    "status": "matched", "equity": "10500.25", "positionCount": 3,
                    "positionDetail": [
                        {"market": "BTC", "size": "0.1", "side": "long"},
                        {"market": "ETH", "size": "2.0", "side": "short"}
                    ],
                    "isReconciled": true, "extra": null
                },
                "actorType": "system", "actorId": "venue-reconciliation", "tokenJti": null,
                "createdAt": "2026-03-28T10:00:00.000000Z", "updatedAt": "2026-03-28T10:00:00.000000Z"
            }
        """.trimIndent()
        val op = arcaJson.decodeFromString<Operation>(json)
        assertEquals(OperationType.FILL, op.type)
        assertEquals(OperationState.COMPLETED, op.state)

        val parsed = op.parsedOutcome
        assertNotNull(parsed)
        assertEquals("matched", (parsed!!["status"] as JsonPrimitive).content)
        assertEquals("10500.25", (parsed["equity"] as JsonPrimitive).content)
        assertEquals(3, (parsed["positionCount"] as JsonPrimitive).int)
        assertEquals(true, (parsed["isReconciled"] as JsonPrimitive).boolean)
        assertEquals(JsonNull, parsed["extra"])

        val positions = parsed["positionDetail"] as JsonArray
        assertEquals(2, positions.size)
        val first = positions[0] as JsonObject
        assertEquals("BTC", (first["market"] as JsonPrimitive).content)
        assertEquals("long", (first["side"] as JsonPrimitive).content)
    }

    @Test
    fun operationDecodingWithNilParsedOutcome() {
        val json = """
            {
                "id": "op_01abc", "realmId": "rlm_01def", "path": "/op/transfer/1", "type": "transfer",
                "state": "completed", "sourceArcaPath": "/wallets/a", "targetArcaPath": "/wallets/b",
                "input": null, "outcome": null, "parsedOutcome": null, "actorType": "BUILDER",
                "actorId": "usr_01xyz", "tokenJti": null,
                "createdAt": "2026-03-07T10:00:00.000000Z", "updatedAt": "2026-03-07T10:01:00.000000Z"
            }
        """.trimIndent()
        val op = arcaJson.decodeFromString<Operation>(json)
        assertNull(op.parsedOutcome)
    }

    @Test
    fun operationDecodingWithStringOnlyParsedOutcome() {
        val json = """
            {
                "id": "op_01abc", "realmId": "rlm_01def", "path": "/op/transfer/1", "type": "transfer",
                "state": "completed", "sourceArcaPath": null, "targetArcaPath": null,
                "input": null, "outcome": null,
                "parsedOutcome": {"status": "ok", "amount": "100.50"},
                "actorType": "BUILDER", "actorId": "usr_01xyz", "tokenJti": null,
                "createdAt": "2026-03-07T10:00:00.000000Z", "updatedAt": "2026-03-07T10:01:00.000000Z"
            }
        """.trimIndent()
        val op = arcaJson.decodeFromString<Operation>(json)
        val parsed = op.parsedOutcome
        assertNotNull(parsed)
        assertEquals("ok", (parsed!!["status"] as JsonPrimitive).content)
        assertEquals("100.50", (parsed["amount"] as JsonPrimitive).content)
    }

    @Test
    fun operationStateTerminal() {
        assertTrue(OperationState.COMPLETED.isTerminal)
        assertTrue(OperationState.FAILED.isTerminal)
        assertTrue(OperationState.EXPIRED.isTerminal)
        assertFalse(OperationState.PENDING.isTerminal)
    }

    @Test
    fun fundingOperationDecoding() {
        val json = """
            {
                "id": "op_fund01", "realmId": "rlm_01def",
                "path": "/op/funding/exchanges/main/BTC/op_fund01", "type": "funding",
                "state": "completed", "sourceArcaPath": "/exchanges/main", "targetArcaPath": null,
                "input": null, "outcome": "{\"status\":\"completed\"}", "actorType": "system",
                "actorId": "sim-exchange", "tokenJti": null,
                "createdAt": "2026-03-25T12:00:00.000000Z", "updatedAt": "2026-03-25T12:00:00.000000Z"
            }
        """.trimIndent()
        val op = arcaJson.decodeFromString<Operation>(json)
        assertEquals(OperationType.FUNDING, op.type)
        assertEquals(OperationState.COMPLETED, op.state)
    }

    // MARK: - Balance

    @Test
    fun balanceDecoding() {
        val json = """
            {
                "id": "bal_01abc", "arcaId": "obj_01def", "denomination": "USD", "amount": "1000.50",
                "arriving": "200.00", "settled": "800.50", "departing": "0.00", "total": "1000.50"
            }
        """.trimIndent()
        val balance = arcaJson.decodeFromString<ArcaBalance>(json)
        assertEquals("USD", balance.denomination)
        assertEquals("1000.50", balance.amount)
        assertEquals("200.00", balance.arriving)
        assertEquals("800.50", balance.settled)
        assertEquals("0.00", balance.departing)
        assertEquals("1000.50", balance.total)
    }

    @Test
    fun balanceDecodingBasicShape() {
        val json = """
            { "id": "bal_01abc", "arcaId": "obj_01def", "denomination": "USD", "amount": "500.00" }
        """.trimIndent()
        val balance = arcaJson.decodeFromString<ArcaBalance>(json)
        assertEquals("bal_01abc", balance.id?.value)
        assertEquals("obj_01def", balance.arcaId?.value)
        assertEquals("USD", balance.denomination)
        assertEquals("500.00", balance.amount)
        assertNull(balance.arriving)
        assertNull(balance.settled)
        assertNull(balance.departing)
        assertNull(balance.total)
    }

    @Test
    fun balanceDecodingSummaryShape() {
        val json = """
            { "denomination": "USD", "arriving": "50.00", "settled": "800.00", "departing": "100.00", "total": "950.00" }
        """.trimIndent()
        val balance = arcaJson.decodeFromString<ArcaBalance>(json)
        assertNull(balance.id)
        assertNull(balance.arcaId)
        assertNull(balance.amount)
        assertEquals("USD", balance.denomination)
        assertEquals("50.00", balance.arriving)
        assertEquals("800.00", balance.settled)
        assertEquals("100.00", balance.departing)
        assertEquals("950.00", balance.total)
    }

    @Test
    fun objectDetailResponseBasicBalances() {
        val json = """
            {
                "object": {
                    "id": "obj_01abc", "realmId": "rlm_01def", "path": "/wallets/main", "type": "denominated",
                    "denomination": "USD", "status": "active", "metadata": null, "deletedAt": null,
                    "systemOwned": false, "createdAt": "2026-03-28T10:00:00.000000Z", "updatedAt": "2026-03-28T10:00:00.000000Z"
                },
                "operations": [], "events": [], "deltas": [],
                "balances": [ {"id": "bal_01", "arcaId": "obj_01abc", "denomination": "USD", "amount": "1000"} ]
            }
        """.trimIndent()
        val detail = arcaJson.decodeFromString<ArcaObjectDetailResponse>(json)
        assertEquals(1, detail.balances.size)
        assertEquals("USD", detail.balances[0].denomination)
        assertEquals("1000", detail.balances[0].amount)
        assertNull(detail.balances[0].arriving)
        assertNull(detail.reservedBalances)
        assertNull(detail.positions)
    }

    // MARK: - StateDelta

    @Test
    fun stateDeltaDecoding() {
        val json = """
            {
                "id": "dlt_01abc", "realmId": "rlm_01def", "eventId": "evt_01ghi", "arcaPath": "/wallets/main",
                "deltaType": "balance_change", "beforeValue": "1000", "afterValue": "500",
                "createdAt": "2026-03-07T10:00:00.000000Z"
            }
        """.trimIndent()
        val delta = arcaJson.decodeFromString<StateDelta>(json)
        assertEquals(DeltaType.BALANCE_CHANGE, delta.deltaType)
        assertEquals("1000", delta.beforeValue)
        assertEquals("500", delta.afterValue)
    }

    @Test
    fun stateDeltaBalanceAdjustmentDecoding() {
        val json = """
            {
                "id": "dlt_adj01", "realmId": "rlm_01def", "eventId": "evt_adj01", "arcaPath": "/exchanges/main",
                "deltaType": "balance_adjustment", "beforeValue": "9800.50", "afterValue": "9850.75",
                "createdAt": "2026-03-28T10:00:00.000000Z"
            }
        """.trimIndent()
        val delta = arcaJson.decodeFromString<StateDelta>(json)
        assertEquals(DeltaType.BALANCE_ADJUSTMENT, delta.deltaType)
        assertEquals("9800.50", delta.beforeValue)
        assertEquals("9850.75", delta.afterValue)
    }

    @Test
    fun deltaTypeUnknownValueDoesNotCrash() {
        val json = """
            {
                "id": "dlt_future01", "realmId": "rlm_01def", "eventId": "evt_future01", "arcaPath": "/wallets/main",
                "deltaType": "some_future_delta_type", "beforeValue": null, "afterValue": "100",
                "createdAt": "2026-03-28T10:00:00.000000Z"
            }
        """.trimIndent()
        val delta = arcaJson.decodeFromString<StateDelta>(json)
        assertEquals(DeltaType("some_future_delta_type"), delta.deltaType)
    }

    @Test
    fun deltaTypeUnknownRoundTrips() {
        val original = DeltaType("custom_type")
        val data = arcaJson.encodeToString(original)
        assertEquals(original, arcaJson.decodeFromString<DeltaType>(data))
    }

    @Test
    fun operationDetailWithBalanceAdjustmentDelta() {
        val json = """
            {
                "operation": {
                    "id": "op_adj01", "realmId": "rlm_01def", "path": "/op/adjustment/exchanges/main/op_adj01",
                    "type": "adjustment", "state": "completed", "sourceArcaPath": null, "targetArcaPath": "/exchanges/main",
                    "input": null, "outcome": "{\"type\":\"positive_drift\"}", "actorType": "system",
                    "actorId": "venue_reconciliation", "tokenJti": null,
                    "createdAt": "2026-03-28T10:00:00.000000Z", "updatedAt": "2026-03-28T10:00:00.000000Z"
                },
                "events": [],
                "deltas": [
                    {"id": "dlt_01", "realmId": "rlm_01def", "arcaPath": "/exchanges/main", "deltaType": "balance_change", "beforeValue": "9800", "afterValue": "9850", "createdAt": "2026-03-28T10:00:00.000000Z"},
                    {"id": "dlt_02", "realmId": "rlm_01def", "arcaPath": "/exchanges/main", "deltaType": "status_change", "beforeValue": null, "afterValue": "active", "createdAt": "2026-03-28T10:00:00.000000Z"},
                    {"id": "dlt_03", "realmId": "rlm_01def", "eventId": "evt_adj01", "arcaPath": "/exchanges/main", "deltaType": "balance_adjustment", "beforeValue": "9800", "afterValue": "9850", "createdAt": "2026-03-28T10:00:00.000000Z"}
                ]
            }
        """.trimIndent()
        val detail = arcaJson.decodeFromString<OperationDetailResponse>(json)
        assertEquals(OperationType.ADJUSTMENT, detail.operation.type)
        assertEquals(3, detail.deltas.size)
        assertEquals(DeltaType.BALANCE_CHANGE, detail.deltas[0].deltaType)
        assertEquals(DeltaType.STATUS_CHANGE, detail.deltas[1].deltaType)
        assertEquals(DeltaType.BALANCE_ADJUSTMENT, detail.deltas[2].deltaType)
    }

    @Test
    fun stateDeltaLabelsChangeDecoding() {
        val json = """
            {
                "id": "dlt_01abc", "realmId": "rlm_01def", "eventId": "evt_01ghi", "arcaPath": "/wallets/main/.info",
                "deltaType": "labels_change", "beforeValue": "{}", "afterValue": "{\"tier\": \"gold\"}",
                "createdAt": "2026-03-07T10:00:00.000000Z"
            }
        """.trimIndent()
        val delta = arcaJson.decodeFromString<StateDelta>(json)
        assertEquals(DeltaType.LABELS_CHANGE, delta.deltaType)
        assertEquals("{}", delta.beforeValue)
        assertEquals("{\"tier\": \"gold\"}", delta.afterValue)
    }

    // MARK: - Exchange state

    @Test
    fun exchangeStateDecoding() {
        val json = """
            {
                "account": {"id": "act_01abc", "realmId": "rlm_01def", "name": "test-exchange", "createdAt": "2026-03-07T10:00:00.000000Z", "updatedAt": "2026-03-07T10:00:00.000000Z"},
                "marginSummary": {"equity": "10000", "initialMarginUsed": "500", "maintenanceMarginRequired": "100", "availableToWithdraw": "9500", "totalNtlPos": "5000", "totalUnrealizedPnl": "100", "totalRawUsd": "9900"},
                "crossMarginSummary": {"equity": "10000", "initialMarginUsed": "500", "maintenanceMarginRequired": "100", "availableToWithdraw": "9500", "totalNtlPos": "5000", "totalUnrealizedPnl": "100", "totalRawUsd": "9900"},
                "crossMaintenanceMarginUsed": "100", "positions": [], "openOrders": [], "feeRates": null
            }
        """.trimIndent()
        val state = arcaJson.decodeFromString<ExchangeState>(json)
        assertEquals("act_01abc", state.account.id.value)
        assertEquals("10000", state.marginSummary.equity)
        assertEquals("500", state.marginSummary.initialMarginUsed)
        assertEquals("100", state.marginSummary.maintenanceMarginRequired)
        assertEquals("9500", state.marginSummary.availableToWithdraw)
        assertEquals("100", state.crossMaintenanceMarginUsed)
        assertTrue(state.positions.isEmpty())
        assertTrue(state.openOrders.isEmpty())
        assertNull(state.feeRates)
    }

    @Test
    fun exchangeStateDecodingWithPositions() {
        val json = """
            {
                "account": {"id": "act_01abc", "realmId": "rlm_01def", "name": "test-exchange", "createdAt": "2026-03-07T10:00:00.000000Z", "updatedAt": "2026-03-07T10:00:00.000000Z"},
                "marginSummary": {"equity": "10000", "initialMarginUsed": "500", "maintenanceMarginRequired": "100", "availableToWithdraw": "9500", "totalNtlPos": "5000", "totalUnrealizedPnl": "100", "totalRawUsd": "9900"},
                "positions": [
                    {"id": "sps_01abc", "accountId": "act_01abc", "realmId": "rlm_01def", "market": "BTC", "side": "long", "size": "0.1", "entryPrice": "65000", "leverage": 5, "marginUsed": "1300", "liquidationPrice": "52000", "unrealizedPnl": "150.50", "createdAt": "2026-03-07T10:00:00.000000Z", "updatedAt": "2026-03-07T10:05:00.000000Z"}
                ],
                "openOrders": [], "feeRates": null
            }
        """.trimIndent()
        val state = arcaJson.decodeFromString<ExchangeState>(json)
        assertEquals(1, state.positions.size)
        val pos = state.positions[0]
        assertEquals("sps_01abc", pos.id.value)
        assertEquals("act_01abc", pos.accountId?.value)
        assertEquals("rlm_01def", pos.realmId?.value)
        assertEquals("BTC", pos.market)
        assertEquals(PositionSide.LONG, pos.side)
        assertEquals("0.1", pos.size)
        assertEquals("65000", pos.entryPrice)
        assertEquals(5, pos.leverage)
        assertEquals("1300", pos.marginUsed)
        assertEquals("52000", pos.liquidationPrice)
        assertEquals("150.50", pos.unrealizedPnl)
        assertEquals("2026-03-07T10:00:00.000000Z", pos.createdAt)
        assertEquals("2026-03-07T10:05:00.000000Z", pos.updatedAt)
    }

    @Test
    fun positionListResponseDecoding() {
        val json = """
            {
                "positions": [
                    {"id": "sps_01kme4wd4wft3sz9cjaj7vedmb", "accountId": "act_01kmb3yn78ff3vrcseym39hqjv", "realmId": "rlm_01kmb3gpdde24vxnppyc77j08y", "market": "hl:0:BTC", "side": "long", "size": "0.1", "entryPrice": "65000", "leverage": 5, "marginUsed": "1300", "liquidationPrice": "52000", "unrealizedPnl": "150.50", "createdAt": "2026-03-07T10:00:00.000000Z", "updatedAt": "2026-03-07T10:05:00.000000Z"}
                ],
                "total": 1
            }
        """.trimIndent()
        val response = arcaJson.decodeFromString<PositionListResponse>(json)
        assertEquals(1, response.positions.size)
        assertEquals(1, response.total)
        val pos = response.positions[0]
        assertEquals("sps_01kme4wd4wft3sz9cjaj7vedmb", pos.id.value)
        assertEquals("hl:0:BTC", pos.market)
        assertEquals(PositionSide.LONG, pos.side)
        assertEquals("0.1", pos.size)
        assertNull(pos.cumulativeFunding)
    }

    @Test
    fun positionDecodingWithCumulativeFunding() {
        val json = """
            {
                "positions": [
                    {"id": "sps_01kme4wd4wft3sz9cjaj7vedmb", "accountId": "act_01kmb3yn78ff3vrcseym39hqjv", "realmId": "rlm_01kmb3gpdde24vxnppyc77j08y", "market": "hl:0:ETH", "side": "short", "size": "1.0", "entryPrice": "3500", "leverage": 10, "marginUsed": "350", "liquidationPrice": "3850", "unrealizedPnl": "-25.00", "cumulativeFunding": "12.50", "createdAt": "2026-03-07T10:00:00.000000Z", "updatedAt": "2026-03-07T10:05:00.000000Z"}
                ],
                "total": 1
            }
        """.trimIndent()
        val response = arcaJson.decodeFromString<PositionListResponse>(json)
        val pos = response.positions[0]
        assertEquals("12.50", pos.cumulativeFunding)
        assertEquals(PositionSide.SHORT, pos.side)
    }

    @Test
    fun positionListResponseDecodingViaApiEnvelope() {
        val json = """
            {
                "success": true,
                "data": {
                    "positions": [ {"id": "sps_01abc", "accountId": "act_01abc", "realmId": "rlm_01def", "market": "hl:0:BTC", "side": "long", "size": "0.5", "entryPrice": "50000", "leverage": 5, "marginUsed": "5000"} ],
                    "total": 1
                }
            }
        """.trimIndent()
        val envelope = arcaJson.decodeFromString(ApiResponse.serializer(PositionListResponse.serializer()), json)
        assertTrue(envelope.success)
        assertEquals(1, envelope.data?.positions?.size)
        assertEquals("hl:0:BTC", envelope.data?.positions?.get(0)?.market)
    }

    @Test
    fun orderListResponseDecoding() {
        val json = """
            {
                "orders": [
                    {"id": "ord_01abc", "accountId": "act_01abc", "realmId": "rlm_01def", "market": "hl:0:BTC", "side": "sell", "orderType": "LIMIT", "price": "66300", "size": "0.1", "filledSize": "0", "status": "WAITING_FOR_TRIGGER", "reduceOnly": true, "timeInForce": "GTC", "leverage": 5, "isTrigger": true, "triggerPx": "66300", "tpsl": "tp", "sizeToMax": true, "createdAt": "2026-03-28T03:50:00.000000Z", "updatedAt": "2026-03-28T03:50:00.000000Z"}
                ],
                "total": 1
            }
        """.trimIndent()
        val response = arcaJson.decodeFromString<OrderListResponse>(json)
        assertEquals(1, response.orders.size)
        assertEquals(1, response.total)
        val order = response.orders[0]
        assertEquals("ord_01abc", order.id.value)
        assertEquals("hl:0:BTC", order.market)
        assertEquals(OrderSide.SELL, order.side)
        assertEquals(true, order.isTrigger)
        assertEquals("66300", order.triggerPx)
        assertEquals("tp", order.tpsl)
        assertEquals(true, order.sizeToMax)
    }

    @Test
    fun orderListResponseDecodingViaApiEnvelope() {
        val json = """
            {
                "success": true,
                "data": {
                    "orders": [ {"id": "ord_01abc", "market": "hl:0:BTC", "side": "buy", "orderType": "LIMIT", "price": "60000", "size": "0.5", "filledSize": "0", "status": "OPEN", "reduceOnly": false, "timeInForce": "GTC", "leverage": 3} ],
                    "total": 1
                }
            }
        """.trimIndent()
        val envelope = arcaJson.decodeFromString(ApiResponse.serializer(OrderListResponse.serializer()), json)
        assertTrue(envelope.success)
        assertEquals(1, envelope.data?.orders?.size)
        assertEquals("hl:0:BTC", envelope.data?.orders?.get(0)?.market)
    }

    @Test
    fun exchangeStateDecodingWithOpenOrders() {
        val json = """
            {
                "account": {"id": "act_01abc", "realmId": "rlm_01def", "name": "test-exchange", "createdAt": "2026-03-07T10:00:00.000000Z", "updatedAt": "2026-03-07T10:00:00.000000Z"},
                "marginSummary": {"equity": "10000", "initialMarginUsed": "0", "maintenanceMarginRequired": "0", "availableToWithdraw": "10000", "totalNtlPos": "0", "totalUnrealizedPnl": "0", "totalRawUsd": "10000"},
                "positions": [],
                "openOrders": [
                    {"id": "ord_01abc", "accountId": "act_01abc", "realmId": "rlm_01def", "market": "ETH", "side": "buy", "orderType": "LIMIT", "price": "3000", "size": "1.0", "filledSize": "0", "avgFillPrice": null, "status": "OPEN", "reduceOnly": false, "timeInForce": "GTC", "leverage": 3, "builderFeeBps": null, "createdAt": "2026-03-07T10:00:00.000000Z", "updatedAt": "2026-03-07T10:00:00.000000Z"}
                ],
                "feeRates": null
            }
        """.trimIndent()
        val state = arcaJson.decodeFromString<ExchangeState>(json)
        assertEquals(1, state.openOrders.size)
        val order = state.openOrders[0]
        assertEquals("ord_01abc", order.id.value)
        assertEquals("act_01abc", order.accountId?.value)
        assertEquals("rlm_01def", order.realmId?.value)
        assertEquals("ETH", order.market)
        assertEquals(OrderSide.BUY, order.side)
        assertEquals(OrderType.LIMIT, order.orderType)
        assertEquals("3000", order.price)
        assertEquals("1.0", order.size)
        assertEquals("0", order.filledSize)
        assertNull(order.avgFillPrice)
        assertEquals(OrderStatus.OPEN, order.status)
        assertFalse(order.reduceOnly)
        assertEquals(TimeInForce.GTC, order.timeInForce)
        assertEquals(3, order.leverage)
        assertNull(order.builderFeeBps)
    }

    @Test
    fun updateLeverageRequestEncodesLeverageAsInt() {
        val json = arcaJson.encodeToString(LeverageBody(market = "BTC", leverage = 40))
        assertTrue(json.contains("\"leverage\":40"), "leverage must encode as a JSON integer, got: $json")
        assertFalse(json.contains("\"leverage\":\"40\""), "leverage must NOT encode as a JSON string")
    }

    @Test
    fun activeAssetDataDecoding() {
        val json = """
            {
                "market": "BTC", "leverage": { "type": "cross", "value": 5 }, "maxBuySize": "0.1538",
                "maxSellSize": "-0.1538", "maxBuyUsd": "10000", "maxSellUsd": "-10000",
                "availableToTrade": "5000", "markPx": "65000", "feeRate": "0.00045", "maintenanceMarginRate": "0.03"
            }
        """.trimIndent()
        val data = arcaJson.decodeFromString<ActiveAssetData>(json)
        assertEquals("BTC", data.market)
        assertEquals(LeverageType.CROSS, data.leverage.type)
        assertEquals(5, data.leverage.value)
        assertEquals("0.1538", data.maxBuySize)
        assertEquals("5000", data.availableToTrade)
        assertEquals("65000", data.markPx)
        assertEquals("0.03", data.maintenanceMarginRate)
    }

    // MARK: - Aggregation

    @Test
    fun pathAggregationDecoding() {
        val json = """
            {
                "prefix": "/", "totalEquityUsd": "50000", "departingUsd": "1000",
                "breakdown": [ {"asset": "USD", "category": "spot", "amount": "50000", "price": "1", "valueUsd": "50000"} ]
            }
        """.trimIndent()
        val agg = arcaJson.decodeFromString<PathAggregation>(json)
        assertEquals("50000", agg.totalEquityUsd)
        assertEquals(1, agg.breakdown.size)
        assertEquals(AssetCategory.SPOT, agg.breakdown[0].category)
        assertNull(agg.arrivingUsd)
        assertNull(agg.asOf)
    }

    @Test
    fun objectValuationDecodingMissingReservedBalances() {
        val json = """
            {
                "objectId": "obj_01abc", "path": "/users/u1/wallet", "type": "denominated", "denomination": "USD",
                "valueUsd": "1000", "balances": [ {"denomination": "USD", "amount": "1000", "price": "1.0", "valueUsd": "1000"} ]
            }
        """.trimIndent()
        val value = arcaJson.decodeFromString<ObjectValuation>(json)
        assertEquals("obj_01abc", value.objectId.value)
        assertEquals("1000", value.valueUsd)
        assertEquals(1, value.balances.size)
        assertNull(value.reservedBalances)
        assertNull(value.pendingInbound)
        assertNull(value.positions)
    }

    @Test
    fun createWatchResponseDecodingNoReservedBalances() {
        val json = """
            {
                "watchId": "req_01abc",
                "aggregation": {"prefix": "/users/u1/", "totalEquityUsd": "1000", "departingUsd": "0", "arrivingUsd": "0", "breakdown": [ {"asset": "USD", "category": "spot", "amount": "1000", "price": "1.0", "valueUsd": "1000"} ]}
            }
        """.trimIndent()
        val resp = arcaJson.decodeFromString<CreateWatchResponse>(json)
        assertEquals("req_01abc", resp.watchId.value)
        assertEquals("1000", resp.aggregation.totalEquityUsd)
        assertEquals(1, resp.aggregation.breakdown.size)
        assertEquals("1000", resp.aggregation.breakdown[0].valueUsd)
    }

    @Test
    fun pathAggregationRevaluedFromBreakdown() {
        val agg = PathAggregation(
            prefix = "/", totalEquityUsd = "100700", departingUsd = "10", arrivingUsd = "5",
            breakdown = listOf(
                AssetBreakdown("hl:0:BTC", AssetCategory.SPOT, "2", "50000", "100000"),
                AssetBreakdown("hl:0:ETH", AssetCategory.PERP, "1", "3000", "500"),
                AssetBreakdown("ex", AssetCategory.EXCHANGE, "0", null, "200"),
            ),
        )
        val re = agg.revalued(mapOf("hl:0:BTC" to "60000"))
        assertEquals("10", re.departingUsd)
        assertEquals("5", re.arrivingUsd)
        assertEquals("120000", re.breakdown[0].valueUsd)
        assertEquals("60000", re.breakdown[0].price)
        assertEquals("500", re.breakdown[1].valueUsd)
        assertEquals("200", re.breakdown[2].valueUsd)
        assertEquals("120700", re.totalEquityUsd)
    }

    @Test
    fun pathAggregationRevaluedLongPerpFromMids() {
        val agg = PathAggregation(
            prefix = "/", totalEquityUsd = "5000", departingUsd = "0", arrivingUsd = null,
            breakdown = listOf(
                AssetBreakdown("hl:0:BTC", AssetCategory.PERP, "0.5", "55000", "2500", avgEntryPrice = "50000"),
                AssetBreakdown("USD", AssetCategory.SPOT, "2500", "1", "2500"),
            ),
        )
        val re = agg.revalued(mapOf("hl:0:BTC" to "62000", "USD" to "1"))
        assertDecimalEquals("6000", re.breakdown[0].valueUsd)
        assertEquals("62000", re.breakdown[0].price)
        assertDecimalEquals("8500", re.totalEquityUsd)
    }

    @Test
    fun pathAggregationRevaluedShortPerpFromMids() {
        val agg = PathAggregation(
            prefix = "/", totalEquityUsd = "2900", departingUsd = "0", arrivingUsd = null,
            breakdown = listOf(
                AssetBreakdown("hl:0:ETH", AssetCategory.PERP, "2", "3000", "400", avgEntryPrice = "-3200"),
                AssetBreakdown("USD", AssetCategory.SPOT, "2500", "1", "2500"),
            ),
        )
        val re = agg.revalued(mapOf("hl:0:ETH" to "2800", "USD" to "1"))
        assertDecimalEquals("800", re.breakdown[0].valueUsd)
        assertEquals("2800", re.breakdown[0].price)
        assertDecimalEquals("3300", re.totalEquityUsd)
    }

    // MARK: - Summary

    @Test
    fun summaryDecoding() {
        val json = """
            { "objectCount": 5, "operationCount": 20, "eventCount": 50, "pendingOperationCount": 2 }
        """.trimIndent()
        val summary = arcaJson.decodeFromString<ExplorerSummary>(json)
        assertEquals(5, summary.objectCount)
        assertEquals(2, summary.pendingOperationCount)
        assertNull(summary.expiredOperationCount)
    }

    // MARK: - RealmEvent

    @Test
    fun realmEventDecoding() {
        val json = """
            {
                "type": "operation.updated", "realmId": "rlm_01abc", "entityId": "op_01def", "entityPath": "/op/transfer/1",
                "operation": {"id": "op_01def", "realmId": "rlm_01abc", "path": "/op/transfer/1", "type": "transfer", "state": "completed", "sourceArcaPath": "/wallets/a", "targetArcaPath": "/wallets/b", "input": null, "outcome": null, "actorType": null, "actorId": null, "tokenJti": null, "createdAt": "2026-03-07T10:00:00.000000Z", "updatedAt": "2026-03-07T10:01:00.000000Z"}
            }
        """.trimIndent()
        val event = arcaJson.decodeFromString<RealmEvent>(json)
        assertEquals(EventType.OPERATION_UPDATED.wire, event.type)
        assertEquals("op_01def", event.entityId)
        assertNotNull(event.operation)
        assertEquals(OperationState.COMPLETED, event.operation?.state)
        assertNull(event.mids)
    }

    // MARK: - TypedEvent

    @Test
    fun typedEventFromExchangeUpdated() {
        val json = """
            {
                "type": "exchange.updated", "realmId": "rlm_01abc", "entityId": "obj_01def", "entityPath": "/exchanges/main",
                "correlationId": "corr_123", "deliverySeq": 42,
                "exchangeState": {"account": {"id": "act_01abc", "realmId": "rlm_01abc", "name": "test", "createdAt": "2026-03-07T10:00:00.000000Z", "updatedAt": "2026-03-07T10:00:00.000000Z"}, "marginSummary": {"equity": "10000", "initialMarginUsed": "0", "maintenanceMarginRequired": "0", "availableToWithdraw": "10000", "totalNtlPos": "0", "totalUnrealizedPnl": "0", "totalRawUsd": "10000"}, "positions": [], "openOrders": [], "feeRates": null}
            }
        """.trimIndent()
        val typed = TypedEvent.from(arcaJson.decodeFromString<RealmEvent>(json))
        val e = assertInstanceOf(TypedEvent.ExchangeUpdated::class.java, typed)
        assertEquals("act_01abc", e.state.account.id.value)
        assertEquals("rlm_01abc", e.envelope.realmId)
        assertEquals("obj_01def", e.envelope.entityId)
        assertEquals("corr_123", e.envelope.correlationId)
        assertEquals(42, e.envelope.deliverySeq)
    }

    @Test
    fun typedEventFromFillPreview() {
        val json = """
            {
                "type": "fill.previewed", "realmId": "rlm_01abc", "entityId": "obj_01def", "correlationId": "ord_01xyz",
                "sequence": 1, "deliverySeq": 5,
                "fill": {"id": "sf_01abc", "orderId": "ord_01xyz", "accountId": "act_01abc", "realmId": "rlm_01abc", "market": "hl:0:BTC", "side": "buy", "size": "0.1", "price": "65000", "fee": "1.5", "isMaker": false, "isLiquidation": false, "createdAt": "2026-03-27T12:00:00.000000Z"}
            }
        """.trimIndent()
        val typed = TypedEvent.from(arcaJson.decodeFromString<RealmEvent>(json))
        val e = assertInstanceOf(TypedEvent.FillPreview::class.java, typed)
        assertEquals("hl:0:BTC", e.fill.market)
        assertEquals(OrderSide.BUY, e.fill.side)
        assertEquals("ord_01xyz", e.envelope.correlationId)
        assertEquals(1, e.envelope.sequence)
    }

    @Test
    fun typedEventFromFillRecorded() {
        val json = """
            {
                "type": "fill.recorded", "realmId": "rlm_01abc", "entityId": "obj_01def", "correlationId": "ord_01xyz",
                "sequence": 2, "deliverySeq": 6,
                "fill": {"id": "pl_01abc", "operationId": "op_fill_01", "orderId": "ord_01xyz", "market": "hl:0:BTC", "side": "buy", "size": "0.1", "price": "65000", "fee": "1.5", "realizedPnl": "0", "resultingPosition": { "side": "long", "size": "0.1", "entryPx": "65000", "leverage": 5 }, "isLiquidation": false, "createdAt": "2026-03-27T12:00:00.000000Z"}
            }
        """.trimIndent()
        val typed = TypedEvent.from(arcaJson.decodeFromString<RealmEvent>(json))
        val e = assertInstanceOf(TypedEvent.FillRecorded::class.java, typed)
        assertEquals("op_fill_01", e.fill.operationId)
        assertEquals("hl:0:BTC", e.fill.market)
        assertEquals("ord_01xyz", e.envelope.correlationId)
        assertEquals(2, e.envelope.sequence)
    }

    @Test
    fun typedEventFromFunding() {
        val json = """
            {
                "type": "exchange.funding", "realmId": "rlm_01abc", "entityId": "obj_01def", "deliverySeq": 10,
                "funding": {"accountId": "act_01abc", "market": "hl:0:BTC", "side": "long", "size": "0.5", "price": "65000", "fundingRate": "0.0001", "payment": "-0.25"}
            }
        """.trimIndent()
        val typed = TypedEvent.from(arcaJson.decodeFromString<RealmEvent>(json))
        val e = assertInstanceOf(TypedEvent.FundingPaymentEvent::class.java, typed)
        assertEquals("hl:0:BTC", e.payment.market)
        assertEquals("-0.25", e.payment.payment)
        assertEquals("obj_01def", e.envelope.entityId)
    }

    @Test
    fun typedEventFromOperationCreated() {
        val json = """
            {
                "type": "operation.created", "realmId": "rlm_01abc", "entityId": "op_01def", "entityPath": "/op/transfer/1",
                "eventId": "rev_01abc", "deliverySeq": 1,
                "operation": {"id": "op_01def", "realmId": "rlm_01abc", "path": "/op/transfer/1", "type": "transfer", "state": "pending", "sourceArcaPath": "/wallets/a", "targetArcaPath": "/wallets/b", "input": null, "outcome": null, "actorType": null, "actorId": null, "tokenJti": null, "createdAt": "2026-03-27T10:00:00.000000Z", "updatedAt": "2026-03-27T10:00:00.000000Z"}
            }
        """.trimIndent()
        val typed = TypedEvent.from(arcaJson.decodeFromString<RealmEvent>(json))
        val e = assertInstanceOf(TypedEvent.OperationCreated::class.java, typed)
        assertEquals(OperationType.TRANSFER, e.operation.type)
        assertEquals(OperationState.PENDING, e.operation.state)
        assertEquals("rev_01abc", e.envelope.eventId)
        assertEquals("/op/transfer/1", e.envelope.entityPath)
    }

    @Test
    fun typedEventFromTradeExecuted() {
        val json = """
            {
                "type": "trade.executed", "realmId": "rlm_01abc", "entityId": "hl:0:BTC", "market": "hl:0:BTC", "deliverySeq": 50,
                "trade": {"market": "hl:0:BTC", "px": "60500.00", "sz": "0.5", "side": "buy", "time": "2026-04-15T00:00:00.000", "hash": "0xabc123"}
            }
        """.trimIndent()
        val typed = TypedEvent.from(arcaJson.decodeFromString<RealmEvent>(json))
        val e = assertInstanceOf(TypedEvent.TradeExecuted::class.java, typed)
        assertEquals("hl:0:BTC", e.trade.market)
        assertEquals("60500.00", e.trade.trade.px)
        assertEquals("buy", e.trade.trade.side)
        assertEquals("0xabc123", e.trade.trade.hash)
        assertEquals(50, e.envelope.deliverySeq)
    }

    @Test
    fun typedEventUnknownType() {
        val json = """
            { "type": "some.future.event", "realmId": "rlm_01abc", "entityId": "obj_01def", "deliverySeq": 99 }
        """.trimIndent()
        val typed = TypedEvent.from(arcaJson.decodeFromString<RealmEvent>(json))
        val e = assertInstanceOf(TypedEvent.Unknown::class.java, typed)
        assertEquals("some.future.event", e.event.type)
    }

    @Test
    fun typedEventFromRealmCreated() {
        val json = """
            {
                "type": "realm.created", "realmId": "rlm_01abc", "entityId": "rlm_01abc", "deliverySeq": 1,
                "realm": {"id": "rlm_01abc", "orgId": "org_01def", "name": "Test Realm", "slug": "test-realm", "type": "development", "description": null, "settings": null, "archivedAt": null, "createdBy": "usr_01xyz", "createdAt": "2026-03-27T10:00:00.000000Z", "updatedAt": "2026-03-27T10:00:00.000000Z"}
            }
        """.trimIndent()
        val typed = TypedEvent.from(arcaJson.decodeFromString<RealmEvent>(json))
        val e = assertInstanceOf(TypedEvent.RealmCreated::class.java, typed)
        assertEquals("rlm_01abc", e.realm.id.value)
        assertEquals("Test Realm", e.realm.name)
        assertEquals("test-realm", e.realm.slug)
        assertEquals(RealmType.DEVELOPMENT, e.realm.type)
        assertEquals("rlm_01abc", e.envelope.entityId)
        assertEquals(1, e.envelope.deliverySeq)
    }

    @Test
    fun typedEventRealmCreatedMissingRealmFallsToUnknown() {
        val json = """
            { "type": "realm.created", "realmId": "rlm_01abc", "entityId": "rlm_01abc", "deliverySeq": 1 }
        """.trimIndent()
        val typed = TypedEvent.from(arcaJson.decodeFromString<RealmEvent>(json))
        val e = assertInstanceOf(TypedEvent.Unknown::class.java, typed)
        assertEquals("realm.created", e.event.type)
    }

    @Test
    fun typedEventEnvelopeAccessor() {
        val json = """
            { "type": "balance.updated", "realmId": "rlm_01abc", "entityId": "obj_01def", "timestamp": "2026-03-27T10:00:00.000000Z", "deliverySeq": 7 }
        """.trimIndent()
        val typed = TypedEvent.from(arcaJson.decodeFromString<RealmEvent>(json))
        val envelope = typed.envelope
        assertNotNull(envelope)
        assertEquals("rlm_01abc", envelope?.realmId)
        assertEquals("2026-03-27T10:00:00.000000Z", envelope?.timestamp)
        assertEquals(7, envelope?.deliverySeq)
    }

    // MARK: - Deposit response

    @Test
    fun depositResponseDecoding() {
        val json = """
            {
                "operation": {"id": "op_01abc", "realmId": "rlm_01def", "path": "/op/deposit/1", "type": "deposit", "state": "pending", "sourceArcaPath": null, "targetArcaPath": "/wallets/main", "input": null, "outcome": null, "actorType": "BUILDER", "actorId": "usr_01xyz", "tokenJti": null, "createdAt": "2026-03-07T10:00:00.000000Z", "updatedAt": "2026-03-07T10:00:00.000000Z"},
                "poolAddress": "0x1234567890abcdef", "tokenAddress": "0xabcdef1234567890", "chain": "reth", "expiresAt": "2026-03-07T11:00:00.000000Z"
            }
        """.trimIndent()
        val response = arcaJson.decodeFromString<FundAccountResponse>(json)
        assertEquals(OperationType.DEPOSIT, response.operation.type)
        assertEquals("0x1234567890abcdef", response.poolAddress)
        assertEquals("reth", response.chain)
    }

    // MARK: - Market data

    @Test
    fun fillDecodingWithOrderOperationId() {
        val json = """
            {
                "id": "pl_01abc", "operationId": "op_fill_01", "orderOperationId": "op_order_01", "orderId": "ord_01",
                "market": "BTC", "side": "buy", "size": "0.5", "price": "65000", "fee": "1.5", "realizedPnl": "0",
                "resultingPosition": { "side": "long", "size": "0.5", "entryPx": "65000", "leverage": 5 },
                "isLiquidation": false, "createdAt": "2026-03-16T12:00:00.000000Z"
            }
        """.trimIndent()
        val fill = arcaJson.decodeFromString<Fill>(json)
        assertEquals("op_fill_01", fill.operationId)
        assertEquals("op_order_01", fill.orderOperationId)
        assertEquals("ord_01", fill.orderId)
        assertEquals("BTC", fill.market)
    }

    @Test
    fun fillDecodingWithoutOrderOperationId() {
        val json = """
            {
                "id": "pl_02abc", "operationId": "op_fill_02", "market": "ETH",
                "resultingPosition": { "side": "short", "size": "1.0", "leverage": 3 },
                "createdAt": "2026-03-16T12:00:00.000000Z"
            }
        """.trimIndent()
        val fill = arcaJson.decodeFromString<Fill>(json)
        assertEquals("op_fill_02", fill.operationId)
        assertNull(fill.orderOperationId)
        assertNull(fill.orderId)
    }

    @Test
    fun sparklinesResponseDecoding() {
        val json = """
            { "sparklines": { "hl:0:BTC": [60000, 60100, 60050, 60200, 60150], "hl:0:ETH": [3000, 3010, 3005] } }
        """.trimIndent()
        val response = arcaJson.decodeFromString<SparklinesResponse>(json)
        assertEquals(2, response.sparklines.size)
        assertEquals(5, response.sparklines["hl:0:BTC"]?.size)
        assertEquals(3000.0, response.sparklines["hl:0:ETH"]?.first())
    }

    @Test
    fun marketTickerDecoding() {
        val json = """
            {
                "market": "hl:1:TSLA", "dex": "xyz", "symbol": "TSLA", "exchange": "hl", "markPx": "250", "midPx": "250",
                "prevDayPx": "248", "dayNtlVlm": "500000", "priceChange24hPct": "0.8", "openInterest": "1000",
                "funding": "0.0001", "nextFundingTime": 1711900800000, "feeScale": 2.0, "isDelisted": false
            }
        """.trimIndent()
        val ticker = arcaJson.decodeFromString<MarketTicker>(json)
        assertEquals("hl:1:TSLA", ticker.market)
        assertEquals("xyz", ticker.dex)
        assertEquals(2.0, ticker.feeScale)
        assertFalse(ticker.isDelisted)
    }

    @Test
    fun marketTickerDecodingStandardPerp() {
        val json = """
            {
                "market": "hl:0:BTC", "symbol": "BTC", "exchange": "hl", "markPx": "64000", "midPx": "64000",
                "prevDayPx": "63000", "dayNtlVlm": "5000000", "priceChange24hPct": "1.5", "openInterest": "10000",
                "funding": "0.0001", "feeScale": 1.0, "isDelisted": false
            }
        """.trimIndent()
        val ticker = arcaJson.decodeFromString<MarketTicker>(json)
        assertEquals("hl:0:BTC", ticker.market)
        assertEquals(1.0, ticker.feeScale)
        assertNull(ticker.dex)
        assertNull(ticker.nextFundingTime)
    }

    @Test
    fun simBookResponseDecoding() {
        val json = """
            {
                "market": "BTC", "bids": [{"price": "65000", "size": "1.5", "orderCount": 3}],
                "asks": [{"price": "65100", "size": "2.0", "orderCount": 5}], "time": 1709805600
            }
        """.trimIndent()
        val book = arcaJson.decodeFromString<SimBookResponse>(json)
        assertEquals("BTC", book.market)
        assertEquals(1, book.bids.size)
        assertEquals("65000", book.bids[0].price)
        assertEquals(5, book.asks[0].orderCount)
    }

    // MARK: - SimFill preview

    @Test
    fun simFillDecodingPreviewWithoutAccountRealmCreatedAt() {
        val json = """
            { "id": "sf_01abc", "orderId": "ord_01xyz", "market": "hl:0:BTC", "side": "buy", "size": "0.1", "price": "65000", "fee": "1.5", "isMaker": false, "isLiquidation": false }
        """.trimIndent()
        val fill = arcaJson.decodeFromString<SimFill>(json)
        assertEquals("sf_01abc", fill.id.value)
        assertEquals("hl:0:BTC", fill.market)
        assertEquals(OrderSide.BUY, fill.side)
        assertNull(fill.accountId)
        assertNull(fill.realmId)
        assertNull(fill.createdAt)
    }

    @Test
    fun simFillDecodingFullWithAllFields() {
        val json = """
            {
                "id": "sf_01abc", "orderId": "ord_01xyz", "accountId": "act_01abc", "realmId": "rlm_01def",
                "market": "hl:0:BTC", "side": "sell", "size": "0.5", "price": "64000", "fee": "2.0",
                "builderFee": "0.5", "platformFee": "0.3", "realizedPnl": "100.00", "isLiquidation": false,
                "createdAt": "2026-03-28T12:00:00.000000Z"
            }
        """.trimIndent()
        val fill = arcaJson.decodeFromString<SimFill>(json)
        assertEquals("act_01abc", fill.accountId?.value)
        assertEquals("rlm_01def", fill.realmId?.value)
        assertEquals("2026-03-28T12:00:00.000000Z", fill.createdAt)
        assertEquals(OrderSide.SELL, fill.side)
        assertEquals("0.3", fill.platformFee)
    }

    @Test
    fun simFillDecodingPlatformFeeAbsent() {
        val json = """
            { "id": "sf_01abc", "orderId": "ord_01xyz", "market": "hl:0:BTC", "side": "buy", "size": "0.1", "price": "65000", "fee": "1.5", "isLiquidation": false }
        """.trimIndent()
        val fill = arcaJson.decodeFromString<SimFill>(json)
        assertNull(fill.platformFee)
    }

    @Test
    fun realmEventDecodingFillPreviewed() {
        val json = """
            {
                "type": "fill.previewed", "realmId": "rlm_01abc", "entityId": "obj_01def", "deliverySeq": 5,
                "fill": {"id": "sf_01abc", "orderId": "ord_01xyz", "market": "hl:0:BTC", "side": "buy", "size": "0.1", "price": "65000", "fee": "1.5", "isMaker": false, "isLiquidation": false}
            }
        """.trimIndent()
        val event = arcaJson.decodeFromString<RealmEvent>(json)
        assertEquals("fill.previewed", event.type)
        assertNotNull(event.fill)
        assertEquals("hl:0:BTC", event.fill?.market)
        assertNull(event.fill?.accountId)
    }

    // MARK: - Browse response

    @Test
    fun browseResponseDecodingNoPrefix() {
        val json = """
            { "folders": ["/users/", "/exchanges/"], "objects": [], "total": 2 }
        """.trimIndent()
        val response = arcaJson.decodeFromString<ArcaObjectBrowseResponse>(json)
        assertEquals(2, response.folders.size)
        assertTrue(response.objects.isEmpty())
        assertEquals(2, response.total)
    }

    // MARK: - PnlResponse omitempty

    @Test
    fun pnlResponseDecodingWithoutExternalFlows() {
        val json = """
            {
                "prefix": "/", "from": "2026-03-01T00:00:00.000000Z", "to": "2026-03-28T00:00:00.000000Z",
                "startingEquityUsd": "10000", "endingEquityUsd": "10500", "netInflowsUsd": "0", "netOutflowsUsd": "0", "pnlUsd": "500"
            }
        """.trimIndent()
        val pnl = arcaJson.decodeFromString<PnlResponse>(json)
        assertEquals("500", pnl.pnlUsd)
        assertNull(pnl.externalFlows)
    }

    @Test
    fun pnlResponseDecodingWithExternalFlows() {
        val json = """
            {
                "prefix": "/", "from": "2026-03-01T00:00:00.000000Z", "to": "2026-03-28T00:00:00.000000Z",
                "startingEquityUsd": "10000", "endingEquityUsd": "10500", "netInflowsUsd": "1000", "netOutflowsUsd": "0", "pnlUsd": "-500",
                "externalFlows": [ {"operationId": "op_01abc", "type": "deposit", "direction": "inflow", "amount": "1000", "denomination": "USD", "valueUsd": "1000", "timestamp": "2026-03-15T12:00:00.000000Z"} ]
            }
        """.trimIndent()
        val pnl = arcaJson.decodeFromString<PnlResponse>(json)
        assertEquals(1, pnl.externalFlows?.size)
        assertEquals("inflow", pnl.externalFlows?.get(0)?.direction)
    }

    // MARK: - PnlPoint valueUsd

    @Test
    fun pnlPointDecodingWithoutValueUsd() {
        val point = arcaJson.decodeFromString<PnlPoint>("""{ "timestamp": "2026-01-01T00:00:00Z", "pnlUsd": "100.00", "equityUsd": "5100.00" }""")
        assertEquals("100.00", point.pnlUsd)
        assertNull(point.valueUsd)
    }

    @Test
    fun pnlPointDecodingWithValueUsd() {
        val point = arcaJson.decodeFromString<PnlPoint>("""{ "timestamp": "2026-01-01T00:00:00Z", "pnlUsd": "100.00", "equityUsd": "5100.00", "valueUsd": "5100.00" }""")
        assertEquals("5100.00", point.valueUsd)
    }

    @Test
    fun pnlPointMemberwiseInitDefaultValueUsd() {
        val point = PnlPoint(timestamp = "2026-01-01T00:00:00Z", pnlUsd = "200.00", equityUsd = "5200.00")
        assertNull(point.valueUsd)
    }

    @Test
    fun pnlPointMemberwiseInitWithValueUsd() {
        val point = PnlPoint(timestamp = "2026-01-01T00:00:00Z", pnlUsd = "200.00", equityUsd = "5200.00")
        point.valueUsd = "5200.00"
        assertEquals("5200.00", point.valueUsd)
    }

    @Test
    fun pnlAnchorEnum() {
        assertNotEquals(PnlAnchor.ZERO.toString(), PnlAnchor.EQUITY.toString())
    }

    // MARK: - applyEquityAnchor

    @Test
    fun equityAnchorPopulatesValueUsdFromEquityUsd() {
        val points = listOf(
            PnlPoint(timestamp = "2026-01-01T00:00:00Z", pnlUsd = "0.00", equityUsd = "5000.00"),
            PnlPoint(timestamp = "2026-01-01T01:00:00Z", pnlUsd = "500.00", equityUsd = "5500.00"),
        )
        val anchored = applyEquityAnchor(points)
        assertEquals("5000.00", anchored[0].valueUsd)
        assertEquals("5500.00", anchored[1].valueUsd)
    }

    @Test
    fun equityAnchorDoesNotMutateOriginalHistoricalPoints() {
        val original = listOf(
            PnlPoint(timestamp = "2026-01-01T00:00:00Z", pnlUsd = "0.00", equityUsd = "5000.00"),
            PnlPoint(timestamp = "2026-01-01T01:00:00Z", pnlUsd = "100.00", equityUsd = "5100.00"),
        )
        val anchored = applyEquityAnchor(original)
        assertNull(original[0].valueUsd, "Original list elements must not be mutated")
        assertNull(original[1].valueUsd, "Original list elements must not be mutated")
        assertEquals("5000.00", anchored[0].valueUsd, "Anchored copy should have valueUsd set")
    }

    @Test
    fun equityAnchorZeroAnchorDefaultProducesNoValueUsd() {
        val points = listOf(
            PnlPoint(timestamp = "2026-01-01T00:00:00Z", pnlUsd = "100.00", equityUsd = "5100.00"),
            PnlPoint(timestamp = "2026-01-01T01:00:00Z", pnlUsd = "200.00", equityUsd = "5200.00"),
        )
        for (p in points) {
            assertNull(p.valueUsd, "Without applyEquityAnchor, points must not have valueUsd")
        }
    }

    // MARK: - ArcaPositionCurrent entryPx

    @Test
    fun arcaPositionCurrentDecodingEntryPx() {
        val json = """
            { "id": "pos_01abc", "realmId": "rlm_01def", "arcaId": "obj_01ghi", "market": "hl:0:BTC", "side": "long", "size": "0.1", "leverage": 5, "entryPx": "65000", "updatedAt": "2026-03-28T10:00:00.000000Z" }
        """.trimIndent()
        val pos = arcaJson.decodeFromString<ArcaPositionCurrent>(json)
        assertEquals("65000", pos.entryPx)
        assertEquals("hl:0:BTC", pos.market)
    }

    // MARK: - ExchangeState null arrays

    @Test
    fun exchangeStateDecodingNullPositionsAndOrders() {
        val json = """
            {
                "account": {"id": "act_01abc", "realmId": "rlm_01def", "name": "test", "createdAt": "2026-03-28T10:00:00.000000Z", "updatedAt": "2026-03-28T10:00:00.000000Z"},
                "marginSummary": {"equity": "10000", "initialMarginUsed": "0", "maintenanceMarginRequired": "0", "availableToWithdraw": "10000", "totalNtlPos": "0", "totalUnrealizedPnl": "0", "totalRawUsd": "10000"},
                "positions": null, "openOrders": null, "feeRates": null
            }
        """.trimIndent()
        val state = arcaJson.decodeFromString<ExchangeState>(json)
        assertTrue(state.positions.isEmpty())
        assertTrue(state.openOrders.isEmpty())
    }

    @Test
    fun snapshotBalancesDecodingNullPositions() {
        val json = """
            {
                "realmId": "rlm_01abc", "arcaId": "obj_01def", "asOf": "2026-03-28T10:00:00.000000Z",
                "balances": [ {"denomination": "USD", "amount": "1000"} ], "positions": null
            }
        """.trimIndent()
        val snapshot = arcaJson.decodeFromString<SnapshotBalancesResponse>(json)
        assertEquals(1, snapshot.balances.size)
        assertTrue(snapshot.positions.isEmpty())
    }

    // MARK: - ExchangeState.revalued

    private fun makeTestExchangeState(
        positions: List<SimPosition> = emptyList(),
        equity: String = "10000",
        totalRawUsd: String = "10000",
        maintenanceMarginRequired: String = "100",
        pricingMode: PricingMode? = null,
    ): ExchangeState {
        val summary = SimMarginSummary(
            equity = equity, initialMarginUsed = "500", maintenanceMarginRequired = maintenanceMarginRequired,
            availableToWithdraw = "9900", totalNtlPos = "5000", totalUnrealizedPnl = "0", totalRawUsd = totalRawUsd,
        )
        return ExchangeState(
            account = SimAccount(SimAccountId("act_1"), RealmId("rlm_1"), "test", "2026-01-01T00:00:00.000000Z", "2026-01-01T00:00:00.000000Z"),
            marginSummary = summary, crossMarginSummary = summary, crossMaintenanceMarginUsed = "100",
            positions = positions, pricingMode = pricingMode,
        )
    }

    private fun makeTestPosition(market: String, side: PositionSide, size: String, entryPrice: String, marginUsed: String): SimPosition =
        SimPosition(
            id = SimPositionId("sps_1"), accountId = SimAccountId("act_1"), realmId = RealmId("rlm_1"),
            market = market, side = side, size = size, entryPrice = entryPrice, leverage = 10,
            marginUsed = marginUsed, unrealizedPnl = "0", returnOnEquity = "0",
        )

    @Test
    fun exchangeStateRevaluedLongPositionPnl() {
        val pos = makeTestPosition("hl:0:BTC", PositionSide.LONG, "0.5", "50000", "2500")
        val result = makeTestExchangeState(positions = listOf(pos)).revalued(mapOf("hl:0:BTC" to "60000"))
        assertEquals("5000", result.positions[0].unrealizedPnl)
        assertEquals("30000", result.positions[0].positionValue)
    }

    @Test
    fun exchangeStateRevaluedShortPositionPnl() {
        val pos = makeTestPosition("hl:0:ETH", PositionSide.SHORT, "2", "3200", "1280")
        val result = makeTestExchangeState(positions = listOf(pos)).revalued(mapOf("hl:0:ETH" to "3000"))
        assertEquals("400", result.positions[0].unrealizedPnl)
    }

    @Test
    fun exchangeStateRevaluedReturnOnEquity() {
        val pos = makeTestPosition("hl:0:BTC", PositionSide.LONG, "1", "50000", "5000")
        val result = makeTestExchangeState(positions = listOf(pos)).revalued(mapOf("hl:0:BTC" to "55000"))
        assertDecimalEquals("1", result.positions[0].returnOnEquity)
    }

    @Test
    fun exchangeStateRevaluedMarginSummaryRecomputed() {
        val pos1 = makeTestPosition("hl:0:BTC", PositionSide.LONG, "0.5", "50000", "2500")
        val pos2 = makeTestPosition("hl:0:ETH", PositionSide.SHORT, "2", "3200", "1280")
        val result = makeTestExchangeState(positions = listOf(pos1, pos2)).revalued(mapOf("hl:0:BTC" to "60000", "hl:0:ETH" to "3000"))
        assertEquals("5400", result.marginSummary.totalUnrealizedPnl)
        assertEquals("15400", result.marginSummary.equity)
        assertEquals("15300", result.marginSummary.availableToWithdraw)
    }

    @Test
    fun exchangeStateRevaluedCrossMarginSummaryAlsoRevalued() {
        val pos = makeTestPosition("hl:0:BTC", PositionSide.LONG, "1", "50000", "5000")
        val result = makeTestExchangeState(positions = listOf(pos)).revalued(mapOf("hl:0:BTC" to "55000"))
        assertEquals("5000", result.crossMarginSummary?.totalUnrealizedPnl)
        assertEquals("15000", result.crossMarginSummary?.equity)
    }

    @Test
    fun exchangeStateRevaluedPreservesWhenMidMissing() {
        val pos = makeTestPosition("hl:0:BTC", PositionSide.LONG, "1", "50000", "5000")
        val result = makeTestExchangeState(positions = listOf(pos)).revalued(mapOf("hl:0:ETH" to "3000"))
        assertEquals("0", result.positions[0].unrealizedPnl)
    }

    @Test
    fun exchangeStateRevaluedClearsError() {
        val pos = SimPosition(
            id = SimPositionId("sps_1"), accountId = SimAccountId("act_1"), realmId = RealmId("rlm_1"),
            market = "hl:0:BTC", side = PositionSide.LONG, size = "1", entryPrice = "50000", leverage = 10,
            marginUsed = "5000", error = "market_data_unavailable",
        )
        val result = makeTestExchangeState(positions = listOf(pos)).revalued(mapOf("hl:0:BTC" to "55000"))
        assertNull(result.positions[0].error)
        assertEquals("5000", result.positions[0].unrealizedPnl)
    }

    @Test
    fun exchangeStateRevaluedPreservesStructuralFields() {
        val pos = makeTestPosition("hl:0:BTC", PositionSide.LONG, "1", "50000", "5000")
        val result = makeTestExchangeState(positions = listOf(pos)).revalued(mapOf("hl:0:BTC" to "55000"))
        assertEquals("act_1", result.account.id.value)
        assertEquals(0, result.openOrders.size)
        assertEquals("500", result.marginSummary.initialMarginUsed)
        assertEquals("10000", result.marginSummary.totalRawUsd)
    }

    @Test
    fun exchangeStateRevaluedEmptyPositions() {
        val result = makeTestExchangeState(positions = emptyList()).revalued(mapOf("hl:0:BTC" to "60000"))
        assertTrue(result.positions.isEmpty())
        assertEquals("0", result.marginSummary.totalUnrealizedPnl)
        assertEquals("10000", result.marginSummary.equity)
    }

    @Test
    fun exchangeStateRevaluedIdempotent() {
        val pos = makeTestPosition("hl:0:BTC", PositionSide.LONG, "1", "50000", "5000")
        val mids = mapOf("hl:0:BTC" to "55000")
        val first = makeTestExchangeState(positions = listOf(pos)).revalued(mids)
        val second = first.revalued(mids)
        assertEquals(first.marginSummary.equity, second.marginSummary.equity)
        assertEquals(first.positions[0].unrealizedPnl, second.positions[0].unrealizedPnl)
    }

    @Test
    fun exchangeStateRevaluedFloorsAvailableToWithdrawAtZero() {
        val pos = makeTestPosition("hl:0:BTC", PositionSide.LONG, "1", "50000", "5000")
        val state = makeTestExchangeState(positions = listOf(pos), equity = "100", totalRawUsd = "100", maintenanceMarginRequired = "200")
        val result = state.revalued(mapOf("hl:0:BTC" to "50000"))
        assertEquals("0", result.marginSummary.availableToWithdraw)
    }

    // MARK: - Server-authoritative pricing

    private fun exchangeObjectValuationJson(pricingMode: String?): String {
        val pmLine = pricingMode?.let { ",\n  \"pricingMode\": \"$it\"" } ?: ""
        return """
            {
              "objectId": "obj_x", "path": "/u/x/ex", "type": "exchange", "denomination": "USD", "valueUsd": "10000",
              "balances": [{"denomination":"USD","amount":"10000","price":"1","valueUsd":"10000"}],
              "positions": [{"market":"hl:0:BTC","side":"long","size":"1","entryPrice":"50000","markPrice":"50000","unrealizedPnl":"0","valueUsd":"0"}]$pmLine
            }
        """.trimIndent()
    }

    @Test
    fun objectValuationPricingModeDecodesServerClientAbsent() {
        assertEquals(PricingMode.SERVER, arcaJson.decodeFromString<ObjectValuation>(exchangeObjectValuationJson("server")).pricingMode)
        assertEquals(PricingMode.CLIENT, arcaJson.decodeFromString<ObjectValuation>(exchangeObjectValuationJson("client")).pricingMode)
        assertNull(arcaJson.decodeFromString<ObjectValuation>(exchangeObjectValuationJson(null)).pricingMode)
    }

    @Test
    fun objectValuationRevaluedServerModeIsNoOp() {
        val server = arcaJson.decodeFromString<ObjectValuation>(exchangeObjectValuationJson("server"))
        val re = server.revalued(mapOf("hl:0:BTC" to "60000"))
        assertEquals("10000", re.valueUsd, "server-authoritative valuation is trusted verbatim")
        assertEquals("50000", re.positions?.first()?.markPrice)
        assertEquals("0", re.positions?.first()?.unrealizedPnl)
        assertEquals(PricingMode.SERVER, re.pricingMode)
    }

    @Test
    fun objectValuationRevaluedClientAndAbsentAreIdenticalAndRecompute() {
        val mids = mapOf("hl:0:BTC" to "60000")
        val reClient = arcaJson.decodeFromString<ObjectValuation>(exchangeObjectValuationJson("client")).revalued(mids)
        val reAbsent = arcaJson.decodeFromString<ObjectValuation>(exchangeObjectValuationJson(null)).revalued(mids)
        val reServer = arcaJson.decodeFromString<ObjectValuation>(exchangeObjectValuationJson("server")).revalued(mids)
        assertEquals(reClient.valueUsd, reAbsent.valueUsd)
        assertEquals(reClient.positions?.first()?.unrealizedPnl, reAbsent.positions?.first()?.unrealizedPnl)
        assertDecimalEquals("20000", reClient.valueUsd)
        assertNotEquals(reClient.valueUsd, reServer.valueUsd)
    }

    @Test
    fun objectValuationRevaluedDenominatedServerModeIsNoOp() {
        val json = """
            {
              "objectId": "obj_d", "path": "/u/x/wallet", "type": "denominated", "denomination": "hl:0:BTC", "valueUsd": "50000",
              "balances": [{"denomination":"hl:0:BTC","amount":"1","price":"50000","valueUsd":"50000"}], "pricingMode": "server"
            }
        """.trimIndent()
        val re = arcaJson.decodeFromString<ObjectValuation>(json).revalued(mapOf("hl:0:BTC" to "60000"))
        assertEquals("50000", re.valueUsd)
        assertEquals("50000", re.balances.first().price)
        assertEquals("50000", re.balances.first().valueUsd)
    }

    @Test
    fun pathAggregationPricingModeDecodesServerClientAbsent() {
        fun aggJson(pm: String?): String {
            val pmLine = pm?.let { ",\n  \"pricingMode\": \"$it\"" } ?: ""
            return """
                {
                  "prefix": "/", "totalEquityUsd": "100000", "departingUsd": "0",
                  "breakdown": [{"asset":"hl:0:BTC","category":"spot","amount":"2","price":"50000","valueUsd":"100000"}]$pmLine
                }
            """.trimIndent()
        }
        assertEquals(PricingMode.SERVER, arcaJson.decodeFromString<PathAggregation>(aggJson("server")).pricingMode)
        assertEquals(PricingMode.CLIENT, arcaJson.decodeFromString<PathAggregation>(aggJson("client")).pricingMode)
        assertNull(arcaJson.decodeFromString<PathAggregation>(aggJson(null)).pricingMode)
    }

    @Test
    fun pathAggregationRevaluedServerModeIsNoOp() {
        val agg = PathAggregation(
            prefix = "/", totalEquityUsd = "100000", departingUsd = "0", arrivingUsd = null,
            breakdown = listOf(AssetBreakdown("hl:0:BTC", AssetCategory.SPOT, "2", "50000", "100000")),
            pricingMode = PricingMode.SERVER,
        )
        val re = agg.revalued(mapOf("hl:0:BTC" to "60000"))
        assertEquals("100000", re.totalEquityUsd)
        assertEquals("100000", re.breakdown[0].valueUsd)
        assertEquals("50000", re.breakdown[0].price)
        assertEquals(PricingMode.SERVER, re.pricingMode)
    }

    @Test
    fun pathAggregationRevaluedClientAndAbsentAreIdenticalAndRecompute() {
        fun makeAgg(pm: PricingMode?): PathAggregation = PathAggregation(
            prefix = "/", totalEquityUsd = "100000", departingUsd = "0", arrivingUsd = null,
            breakdown = listOf(AssetBreakdown("hl:0:BTC", AssetCategory.SPOT, "2", "50000", "100000")),
            pricingMode = pm,
        )
        val mids = mapOf("hl:0:BTC" to "60000")
        val reClient = makeAgg(PricingMode.CLIENT).revalued(mids)
        val reAbsent = makeAgg(null).revalued(mids)
        assertEquals(reClient.totalEquityUsd, reAbsent.totalEquityUsd)
        assertEquals(reClient.breakdown[0].valueUsd, reAbsent.breakdown[0].valueUsd)
        assertDecimalEquals("120000", reClient.breakdown[0].valueUsd)
    }

    @Test
    fun exchangeStatePricingModeDecodesServerClientAbsent() {
        fun stateJson(pm: String?): String {
            val pmLine = pm?.let { ",\n  \"pricingMode\": \"$it\"" } ?: ""
            return """
                {
                  "account": {"id":"act_1","realmId":"rlm_1","name":"m","createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z"},
                  "marginSummary": {"equity":"10000","initialMarginUsed":"0","maintenanceMarginRequired":"0","availableToWithdraw":"10000","totalNtlPos":"0","totalUnrealizedPnl":"0"},
                  "positions": [], "openOrders": []$pmLine
                }
            """.trimIndent()
        }
        assertEquals(PricingMode.SERVER, arcaJson.decodeFromString<ExchangeState>(stateJson("server")).pricingMode)
        assertEquals(PricingMode.CLIENT, arcaJson.decodeFromString<ExchangeState>(stateJson("client")).pricingMode)
        assertNull(arcaJson.decodeFromString<ExchangeState>(stateJson(null)).pricingMode)
    }

    @Test
    fun exchangeStateRevaluedServerModeIsNoOp() {
        val pos = makeTestPosition("hl:0:BTC", PositionSide.LONG, "1", "50000", "5000")
        val result = makeTestExchangeState(positions = listOf(pos), pricingMode = PricingMode.SERVER).revalued(mapOf("hl:0:BTC" to "60000"))
        assertEquals("0", result.positions[0].unrealizedPnl)
        assertEquals("10000", result.marginSummary.equity)
        assertEquals(PricingMode.SERVER, result.pricingMode)
    }

    // MARK: - Isolated margin & margin mode

    @Test
    fun simPositionDecodingIsolatedFields() {
        val json = """
            {
                "id": "sps_01abc", "accountId": "act_01abc", "realmId": "rlm_01def", "market": "hl:1:CL", "side": "long",
                "size": "1", "entryPrice": "50", "leverage": 5, "marginUsed": "10", "marginMode": "isolated",
                "isolatedMargin": "125", "liquidationPrice": "40"
            }
        """.trimIndent()
        val pos = arcaJson.decodeFromString<SimPosition>(json)
        assertEquals(MarginMode.ISOLATED, pos.marginMode)
        assertEquals("125", pos.isolatedMargin)
    }

    @Test
    fun simPositionDecodingCrossPositionOmitsIsolatedFields() {
        val json = """
            { "id": "sps_02abc", "market": "hl:0:BTC", "side": "long", "size": "0.1", "entryPrice": "65000", "leverage": 5, "marginUsed": "1300" }
        """.trimIndent()
        val pos = arcaJson.decodeFromString<SimPosition>(json)
        assertEquals(MarginMode.CROSS, pos.marginMode)
        assertNull(pos.isolatedMargin)
    }

    @Test
    fun leverageSettingDecodingWithMarginMode() {
        val setting = arcaJson.decodeFromString<LeverageSetting>("""{ "market": "hl:1:CL", "leverage": 5, "marginMode": "isolated" }""")
        assertEquals("hl:1:CL", setting.market)
        assertEquals(5, setting.leverage)
        assertEquals(MarginMode.ISOLATED, setting.marginMode)
    }

    @Test
    fun updateIsolatedMarginResponseDecoding() {
        val json = """
            { "accountId": "act_01abc", "market": "hl:1:CL", "isolatedMargin": "125", "liquidationPrice": "50" }
        """.trimIndent()
        val resp = arcaJson.decodeFromString<UpdateIsolatedMarginResponse>(json)
        assertEquals("act_01abc", resp.accountId)
        assertEquals("hl:1:CL", resp.market)
        assertEquals("125", resp.isolatedMargin)
        assertEquals("50", resp.liquidationPrice)
    }

    @Test
    fun setMarginModeResponseDecoding() {
        val resp = arcaJson.decodeFromString<SetMarginModeResponse>("""{ "accountId": "act_01abc", "market": "hl:0:BTC", "marginMode": "isolated" }""")
        assertEquals("act_01abc", resp.accountId)
        assertEquals("hl:0:BTC", resp.market)
        assertEquals(MarginMode.ISOLATED, resp.marginMode)
    }

    @Test
    fun fundingHistoryResponseDecoding() {
        // Market-wide SETTLED funding history (getFundingHistory). The second
        // observation omits premium and source to exercise the optional fields.
        val json = """
            {
                "market": "hl:0:BTC",
                "funding": [
                    { "t": 1700000000000, "fundingRate": "0.0000125", "premium": "0.00008", "s": "hl" },
                    { "t": 1700003600000, "fundingRate": "0.0000130" }
                ]
            }
        """.trimIndent()
        val resp = arcaJson.decodeFromString<FundingHistoryResponse>(json)
        assertEquals("hl:0:BTC", resp.market)
        assertEquals(2, resp.funding.size)

        assertEquals(1700000000000L, resp.funding[0].t)
        assertEquals("0.0000125", resp.funding[0].fundingRate)
        assertEquals("0.00008", resp.funding[0].premium)
        assertEquals("hl", resp.funding[0].s)

        // Optional premium/source absent -> null, not empty string.
        assertEquals(1700003600000L, resp.funding[1].t)
        assertEquals("0.0000130", resp.funding[1].fundingRate)
        assertNull(resp.funding[1].premium)
        assertNull(resp.funding[1].s)
    }

    @Test
    fun fundingHistoryEmptyResponseDecoding() {
        // 200 + empty list is the canonical "no history" response.
        val resp = arcaJson.decodeFromString<FundingHistoryResponse>("""{ "market": "hl:0:BTC", "funding": [] }""")
        assertEquals("hl:0:BTC", resp.market)
        assertTrue(resp.funding.isEmpty())
    }
}
