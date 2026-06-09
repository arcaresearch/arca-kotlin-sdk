package network.arca.sdk

import network.arca.sdk.models.FillContext
import network.arca.sdk.models.Operation
import network.arca.sdk.models.OperationContext
import network.arca.sdk.models.OperationState
import network.arca.sdk.models.OperationType
import network.arca.sdk.models.TransferContext
import network.arca.sdk.models.TransferDirection
import network.arca.sdk.models.counterpartyLabel
import network.arca.sdk.models.transferAmount
import network.arca.sdk.models.transferDenomination
import network.arca.sdk.models.transferDirection
import network.arca.sdk.models.transferFee
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class OperationDisplayTest {

    // MARK: - transferDirection

    @Test
    fun transferDirectionIncoming() {
        val op = makeTransfer(source = "/wallets/main", target = "/exchanges/strat-1")
        assertEquals(TransferDirection.INCOMING, op.transferDirection("/exchanges/strat-1"))
    }

    @Test
    fun transferDirectionOutgoing() {
        val op = makeTransfer(source = "/exchanges/strat-1", target = "/wallets/main")
        assertEquals(TransferDirection.OUTGOING, op.transferDirection("/exchanges/strat-1"))
    }

    @Test
    fun transferDirectionNullForNonTransfer() {
        val op = makeOperation(type = OperationType.FILL, source = "/exchanges/main", target = null)
        assertNull(op.transferDirection("/exchanges/main"))
    }

    @Test
    fun transferDirectionNullWhenNoPathMatch() {
        val op = makeTransfer(source = "/wallets/a", target = "/wallets/b")
        assertNull(op.transferDirection("/exchanges/strat-1"))
    }

    // MARK: - counterpartyLabel

    @Test
    fun counterpartyLabelVault() {
        val op = makeTransfer(source = "/wallets/main", target = "/exchanges/strat-1")
        assertEquals("Vault", op.counterpartyLabel("/exchanges/strat-1"))
    }

    @Test
    fun counterpartyLabelStrategyName() {
        val op = makeTransfer(source = "/exchanges/strat-1", target = "/wallets/main")
        assertEquals("strat-1", op.counterpartyLabel("/wallets/main"))
    }

    @Test
    fun counterpartyLabelNullForNonTransfer() {
        val op = makeOperation(type = OperationType.ORDER, source = null, target = "/exchanges/main")
        assertNull(op.counterpartyLabel("/exchanges/main"))
    }

    @Test
    fun counterpartyLabelDeepPath() {
        val op = makeTransfer(source = "/users/abc/wallets/main", target = "/users/abc/exchanges/strat-2")
        assertEquals("Vault", op.counterpartyLabel("/users/abc/exchanges/strat-2"))
    }

    @Test
    fun counterpartyLabelNullWhenNoPathMatch() {
        val op = makeTransfer(source = "/wallets/main", target = "/exchanges/strat-1")
        assertNull(op.counterpartyLabel("/unrelated/path"))
    }

    // MARK: - OperationContext convenience

    @Test
    fun transferContextAccessors() {
        val ctx = OperationContext(
            type = "transfer",
            transfer = TransferContext(
                amount = "5000",
                denomination = "USD",
                sourceArcaPath = "/a",
                targetArcaPath = "/b",
                feeAmount = "0.05",
            ),
        )
        assertEquals("5000", ctx.transferAmount)
        assertEquals("0.05", ctx.transferFee)
        assertEquals("USD", ctx.transferDenomination)
    }

    @Test
    fun transferContextAccessorsNullForFill() {
        val ctx = OperationContext(
            type = "fill",
            fill = FillContext(
                side = "buy",
                size = "1",
                price = "50000",
                market = "BTC",
                realizedPnl = "0",
                fee = "5",
                netBalanceChange = "-50005",
                isLiquidation = false,
            ),
        )
        assertNull(ctx.transferAmount)
        assertNull(ctx.transferFee)
    }

    // MARK: - Helpers

    private fun makeTransfer(source: String, target: String): Operation =
        makeOperation(type = OperationType.TRANSFER, source = source, target = target)

    private fun makeOperation(type: OperationType, source: String?, target: String?): Operation =
        Operation(
            id = OperationId("op_test"),
            realmId = RealmId("realm_test"),
            path = "/op/test/1",
            type = type,
            state = OperationState.COMPLETED,
            sourceArcaPath = source,
            targetArcaPath = target,
            actorType = "builder",
            actorId = UserId("user_test"),
            createdAt = "2026-03-18T00:00:00.000000Z",
            updatedAt = "2026-03-18T00:00:00.000000Z",
        )
}
