package network.arca.sdk.models

import java.time.Instant
import java.time.format.DateTimeParseException

// Money figures (documents/contracts/sdk-money-figure.md): one shape for
// every balance a product shows, projected from one wallet snapshot. A
// figure is Arca's figure verbatim plus the legs of the operations moving
// money into or out of it; nothing here is summed into a figure on the
// client. Conservation (every dollar in exactly one place, counted once) is
// Arca's guarantee at the snapshot's journal position; this file renders it
// and never re-derives it.

/** Which end of a leg this figure is, from the figure's point of view. */
public enum class MoneyDirection(public val wire: String) {
    /** Money on its way into this figure. */
    INCOMING("incoming"),

    /** Money committed to leave, or already gone from, this figure. */
    OUTGOING("outgoing"),
}

/**
 * Where a leg is: [RESERVED] is still inside its source's figure,
 * [IN_TRANSIT] has left its source and not reached its destination, and
 * [EXPECTED] is a provider-confirmed deposit not on chain yet (listed,
 * never counted).
 */
public enum class MoneyLegStage(public val wire: String) {
    RESERVED("reserved"),
    IN_TRANSIT("in_transit"),
    EXPECTED("expected"),
}

/**
 * The other end of a leg, described from the frame alone. [Unknown] is a
 * counterparty the frame does not list (an account past the catalog page or
 * scan, an archived one, another owner's wallet); the leg still renders on
 * the end that is visible.
 */
public sealed class MoneyCounterparty {
    public object Cash : MoneyCounterparty() {
        override fun toString(): String = "Cash"
    }

    public data class Account(val arcaId: String, val name: String?) : MoneyCounterparty()

    public data class External(val address: String?) : MoneyCounterparty()

    public data class Unknown(val arcaId: String?) : MoneyCounterparty()

    /** Whether the money crosses the wallet boundary (a deposit or a send). */
    public val crossesWallet: Boolean get() = this is External

    /** Whether the frame lists this end as one of the wallet's own figures. */
    internal val isListed: Boolean get() = this is Cash || this is Account
}

/**
 * One operation's money moving into or out of a figure: a `moving[]` entry
 * of the snapshot projected onto one end, or a [predicted] leg from an
 * accepted operation the stream has not framed yet.
 */
public data class MoneyLeg(
    val operationId: String,
    /** The snapshot operation's `requestId` (a move's create key) when known. */
    val requestId: String? = null,
    /** `cash_to_trading`, `trading_to_cash`, `trading_to_trading`, `trading_to_external`, `deposit`, `send`, ... */
    val kind: String,
    val direction: MoneyDirection,
    val stage: MoneyLegStage,
    /** What arrives, as a micro-USDC integer string. */
    val amountMicro: String,
    val counterparty: MoneyCounterparty,
    /** True for a leg added by [network.arca.sdk.WalletMoneyWatch.predict] before the stream carried the operation. */
    val predicted: Boolean = false,
    /** The operation's `createdAt` when the snapshot lists it. */
    val startedAt: String? = null,
) {
    val id: String get() = "$operationId:${direction.wire}"
}

/** Why a figure last moved, named only when the frame makes it attributable. */
public data class MoneyChange(
    val cause: Cause,
    /** The signed difference of Arca's figure from the previous frame ("-500000"). A caption, never a figure. */
    val deltaMicro: String,
    /** The journal position of the frame that carried the change. */
    val sequence: Long,
) {
    public sealed class Cause {
        /** An operation touching the figure changed stage, leg or status. */
        public data class Operation(val operationId: String, val kind: String) : Cause()

        /** The venue moved the figure (a fill, a funding payment, a liquidation or its mark) with no operation touching it. */
        public object Venue : Cause() {
            override fun toString(): String = "Venue"
        }
    }
}

/** Which snapshot figure a [MoneyFigure] renders. */
public sealed class MoneyFigureKind {
    /** `totals`: the whole wallet. */
    public object Total : MoneyFigureKind()

    /** `cash.balances`. */
    public object Cash : MoneyFigureKind()

    /** One `accounts[]` entry. */
    public data class Account(val arcaId: String) : MoneyFigureKind()
}

/**
 * A balance as a product shows it: Arca's figure, what of it is spendable,
 * and the operations moving money into or out of it. [settledMicro] is null
 * only when the frame has no figure (an account never observed).
 */
public data class MoneyFigure(
    val kind: MoneyFigureKind,
    /** Arca's figure verbatim: Cash `confirmedMicro`, an account's `balances.totalMicro`, the wallet's `totals.totalMicro`. */
    val settledMicro: String?,
    /** What the figure can pay out now: Cash `availableMicro`, an account's `withdrawableMicro`; null for the total. */
    val availableMicro: String?,
    /** Money on its way in, one leg per operation. */
    val arriving: List<MoneyLeg>,
    /** Money committed to leave or already gone, one leg per operation. */
    val departing: List<MoneyLeg>,
    /** The venue time (accounts) or block time (Cash) the figure describes. */
    val asOf: String?,
    /** False once Arca stopped receiving the figure's source. */
    val current: Boolean,
    /** Where the figure came from: `cash`, an account's `balances.source`, or `totals`. */
    val source: String,
    /**
     * The venue frame's equity for an account, used as is when it is newer
     * than [asOf] and the frame lists no settlement toward the account; null
     * otherwise. Never adds to [settledMicro].
     */
    val revaluedMicro: String? = null,
    /** Why the figure last moved, when the frame can say. */
    val change: MoneyChange? = null,
) {
    /** Whether any leg is in motion (`reserved` or `in_transit`). */
    val inMotion: Boolean get() = (arriving + departing).any { it.stage != MoneyLegStage.EXPECTED }

    /** The caption total of the arriving legs in motion; a sum of annotations for one line of text, never a figure. */
    val arrivingMicro: String get() = Micro.sum(arriving.filter { it.stage != MoneyLegStage.EXPECTED }.map { it.amountMicro })

    /** The caption total of the departing legs. */
    val departingMicro: String get() = Micro.sum(departing.map { it.amountMicro })

    /** The caption total of `expected` deposits. */
    val expectedMicro: String get() = Micro.sum(arriving.filter { it.stage == MoneyLegStage.EXPECTED }.map { it.amountMicro })
}

/** An engineering-only observation about a frame. Never a gate and never a product signal. */
public sealed class MoneyDiagnostic {
    /** `totals.totalMicro` did not equal the figures it was given. */
    public data class TotalsMismatch(val expectedMicro: String, val composedMicro: String) : MoneyDiagnostic()

    /** A predicted leg reached its bound without the stream framing it. */
    public data class PredictionExpired(val requestId: String) : MoneyDiagnostic()

    /** A venue frame older than the account's figure, or racing a pending settlement, was not used. */
    public data class VenueFrameIgnored(val arcaId: String, val reason: String) : MoneyDiagnostic()
}

/**
 * The accepted operation a product tells the watch about before the first
 * frame that carries it, so its legs can show at once. Only what the server
 * returned: its identity, amount and ends. It never changes a figure.
 */
public data class MoneyPrediction(
    val requestId: String,
    val operationId: String? = null,
    val kind: String,
    val amountMicro: String,
    val from: WalletEndpoint,
    val to: WalletEndpoint,
)

/** A venue frame for one account: its equity and the time the venue described, from an exchange-state watch. */
public data class MoneyRevaluation(
    val arcaId: String,
    val equityMicro: String,
    val observedAt: Instant,
) {
    public companion object {
        /** From an exchange-state frame's `marginSummary.equity` (a decimal USD string) and its observation time; null when either is missing. */
        public fun of(arcaId: String, exchangeState: ExchangeState): MoneyRevaluation? {
            val observed = exchangeState.observationTime ?: return null
            val micro = Micro.fromDecimal(exchangeState.marginSummary.equity) ?: return null
            return MoneyRevaluation(arcaId, micro, observed)
        }
    }
}

/**
 * Every money figure of one wallet at one journal position, from one
 * snapshot: the total, Cash, each trading account, and the operations behind
 * the legs. One frame, one value: a product renders all of it or none.
 */
public data class WalletMoney(
    val total: MoneyFigure,
    val cash: MoneyFigure,
    /** Trading accounts keyed by `arcaId`. */
    val accounts: Map<String, MoneyFigure>,
    /** The `arcaId`s in the snapshot's order. */
    val accountOrder: List<String>,
    val operations: List<WalletFundingOperation>,
    val watermark: WalletSnapshotWatermark,
    /** `totals.complete`. */
    val complete: Boolean,
    /** `totals.current`. */
    val current: Boolean,
    val diagnostics: List<MoneyDiagnostic>,
    /** The frame this was projected from. */
    val snapshot: WalletSnapshot,
) {
    /** The figure of one account, null when the snapshot does not list it. */
    public fun account(arcaId: String): MoneyFigure? = accounts[arcaId]

    /** The figures in catalog order. */
    val orderedAccounts: List<MoneyFigure> get() = accountOrder.mapNotNull { accounts[it] }

    public companion object {
        /**
         * Projects a snapshot onto money figures. [previous] names the last
         * frame of the same wallet so [MoneyFigure.change] can be attributed;
         * [predictions] are accepted operations the stream has not framed yet
         * (those it has are dropped here, so a leg is never shown twice);
         * [revaluations] are venue frames per account.
         */
        public fun compose(
            snapshot: WalletSnapshot,
            previous: WalletMoney? = null,
            predictions: List<MoneyPrediction> = emptyList(),
            revaluations: Map<String, MoneyRevaluation> = emptyMap(),
        ): WalletMoney {
            val diagnostics = mutableListOf<MoneyDiagnostic>()
            val legs = legsByEnd(snapshot)
            val sequence = snapshot.watermark.sequence

            val cashLegs = cashOwnLegs(snapshot)
            var cash = MoneyFigure(
                kind = MoneyFigureKind.Cash,
                settledMicro = snapshot.cash.balances.confirmedMicro,
                availableMicro = snapshot.cash.balances.availableMicro,
                arriving = legs.cash.arriving + cashLegs.arriving,
                departing = legs.cash.departing + cashLegs.departing,
                asOf = snapshot.cash.balances.asOf.time,
                current = snapshot.cash.walletState != WalletState.UNAVAILABLE.wire,
                source = "cash",
            )

            val accounts = LinkedHashMap<String, MoneyFigure>()
            val order = ArrayList<String>()
            for (account in snapshot.accounts) {
                val ends = legs.accounts[account.arcaId] ?: LegEnds()
                var figure = MoneyFigure(
                    kind = MoneyFigureKind.Account(account.arcaId),
                    settledMicro = account.balances?.totalMicro,
                    availableMicro = account.balances?.withdrawableMicro,
                    arriving = ends.arriving,
                    departing = ends.departing,
                    asOf = account.asOf,
                    current = account.current,
                    source = account.balances?.source ?: "unavailable",
                )
                revaluations[account.arcaId]?.let { revaluation ->
                    when (val outcome = revaluation(revaluation, account, figure)) {
                        is Revaluation.Success -> figure = figure.copy(revaluedMicro = outcome.micro)
                        is Revaluation.Failure -> diagnostics += MoneyDiagnostic.VenueFrameIgnored(account.arcaId, outcome.reason)
                    }
                }
                accounts[account.arcaId] = figure
                order += account.arcaId
            }

            var total = MoneyFigure(
                kind = MoneyFigureKind.Total,
                settledMicro = snapshot.totals.totalMicro,
                availableMicro = null,
                arriving = legs.total.arriving + cashLegs.arriving,
                departing = legs.total.departing + cashLegs.departing,
                asOf = snapshot.composedAt.takeIf { it.isNotEmpty() },
                current = snapshot.totals.current,
                source = "totals",
            )

            val framed = snapshot.operations.mapNotNull { it.requestIdOrNull }.toSet() + snapshot.operations.map { it.operationId }
            for (prediction in predictions) {
                if (prediction.requestId in framed || (prediction.operationId != null && prediction.operationId in framed)) continue
                val predicted = predictedLegs(prediction, snapshot)
                cash = cash.copy(arriving = cash.arriving + predicted.cash.arriving, departing = cash.departing + predicted.cash.departing)
                for ((arcaId, ends) in predicted.accounts) {
                    val figure = accounts[arcaId] ?: continue
                    accounts[arcaId] = figure.copy(arriving = figure.arriving + ends.arriving, departing = figure.departing + ends.departing)
                }
                total = total.copy(arriving = total.arriving + predicted.total.arriving, departing = total.departing + predicted.total.departing)
            }

            if (snapshot.totals.complete) {
                val composed = composedTotal(snapshot)
                if (composed != null && composed != snapshot.totals.totalMicro) {
                    diagnostics += MoneyDiagnostic.TotalsMismatch(snapshot.totals.totalMicro, composed)
                }
            }

            if (previous != null && previous.snapshot.watermark.sequence < sequence) {
                cash = cash.copy(change = change(previous.cash, cash, snapshot, previous, sequence, null))
                for ((arcaId, figure) in accounts.entries.toList()) {
                    accounts[arcaId] = figure.copy(change = change(previous.accounts[arcaId], figure, snapshot, previous, sequence, snapshot.account(arcaId)))
                }
                total = total.copy(change = change(previous.total, total, snapshot, previous, sequence, null))
            }

            return WalletMoney(
                total = total, cash = cash, accounts = accounts, accountOrder = order, operations = snapshot.operations,
                watermark = snapshot.watermark, complete = snapshot.totals.complete, current = snapshot.totals.current,
                diagnostics = diagnostics, snapshot = snapshot,
            )
        }

        internal class LegEnds(
            val arriving: MutableList<MoneyLeg> = mutableListOf(),
            val departing: MutableList<MoneyLeg> = mutableListOf(),
        )

        internal class LegsByEnd {
            val cash = LegEnds()
            val accounts = LinkedHashMap<String, LegEnds>()
            val total = LegEnds()
        }

        internal fun counterparty(endpoint: WalletEndpoint, snapshot: WalletSnapshot): MoneyCounterparty {
            if (endpoint.isCash || (endpoint.boundaryIdOrNull != null && endpoint.boundaryIdOrNull == snapshot.boundaryId && snapshot.boundaryId.isNotEmpty())) {
                return MoneyCounterparty.Cash
            }
            endpoint.arcaIdOrNull?.let { arcaId ->
                val account = snapshot.account(arcaId)
                return if (account != null) MoneyCounterparty.Account(arcaId, account.name) else MoneyCounterparty.Unknown(arcaId)
            }
            if (endpoint.kind == "external" || endpoint.addressOrNull != null || (endpoint.kind.isNotEmpty() && endpoint.kind != "cash")) {
                return MoneyCounterparty.External(endpoint.addressOrNull)
            }
            return MoneyCounterparty.Unknown(null)
        }

        /**
         * Projects every `moving[]` entry onto the ends the snapshot lists. A
         * `reserved` leg shows at both ends (the money is still inside its
         * source); an `in_transit` leg shows at its destination when the frame
         * lists it, else at its source (a send on its way out of the wallet);
         * an `expected` leg shows only at its destination.
         */
        internal fun legsByEnd(snapshot: WalletSnapshot): LegsByEnd {
            val out = LegsByEnd()
            val operations = snapshot.operations.associateBy { it.operationId }
            for (entry in snapshot.moving) {
                val stage = entry.typedLeg?.let(::stageOf) ?: continue
                val op = operations[entry.operationId]
                val fromParty = counterparty(entry.from, snapshot)
                val toParty = counterparty(entry.to, snapshot)
                val departing = MoneyLeg(entry.operationId, op?.requestIdOrNull, entry.kind, MoneyDirection.OUTGOING, stage, entry.amountMicro, toParty, startedAt = op?.createdAt)
                val arriving = MoneyLeg(entry.operationId, op?.requestIdOrNull, entry.kind, MoneyDirection.INCOMING, stage, entry.amountMicro, fromParty, startedAt = op?.createdAt)
                when (stage) {
                    MoneyLegStage.RESERVED -> {
                        place(departing, fromParty, out)
                        place(arriving, toParty, out)
                    }
                    MoneyLegStage.IN_TRANSIT -> if (toParty.isListed) place(arriving, toParty, out) else place(departing, fromParty, out)
                    MoneyLegStage.EXPECTED -> place(arriving, toParty, out)
                }
                if (toParty.crossesWallet && !fromParty.crossesWallet) out.total.departing += departing
                if (fromParty.crossesWallet && !toParty.crossesWallet) out.total.arriving += arriving
            }
            return out
        }

        /**
         * The Cash read model's own in-flight money: a send in `cash.operations`
         * is `reserved` while `sending` and `in_transit` while `confirming`;
         * `pendingInMicro` is one arriving leg from outside the wallet.
         */
        internal fun cashOwnLegs(snapshot: WalletSnapshot): LegEnds {
            val ends = LegEnds()
            for (op in snapshot.cash.operations) {
                if (op.kind != "send") continue
                val stage = when (op.typedState) {
                    WalletOperationState.SENDING -> MoneyLegStage.RESERVED
                    WalletOperationState.CONFIRMING -> MoneyLegStage.IN_TRANSIT
                    else -> continue
                }
                ends.departing += MoneyLeg(op.id, op.requestId, "send", MoneyDirection.OUTGOING, stage, op.amountMicro, MoneyCounterparty.External(op.destination), startedAt = op.startedAt)
            }
            val pending = Micro.parse(snapshot.cash.balances.pendingInMicro)
            if (pending != null && pending > 0) {
                ends.arriving += MoneyLeg("cash:pending-in", null, "deposit", MoneyDirection.INCOMING, MoneyLegStage.IN_TRANSIT, snapshot.cash.balances.pendingInMicro, MoneyCounterparty.External(null))
            }
            return ends
        }

        internal fun place(leg: MoneyLeg, party: MoneyCounterparty, legs: LegsByEnd) {
            when (party) {
                is MoneyCounterparty.Cash -> if (leg.direction == MoneyDirection.OUTGOING) legs.cash.departing += leg else legs.cash.arriving += leg
                is MoneyCounterparty.Account -> {
                    val ends = legs.accounts.getOrPut(party.arcaId) { LegEnds() }
                    if (leg.direction == MoneyDirection.OUTGOING) ends.departing += leg else ends.arriving += leg
                }
                is MoneyCounterparty.External, is MoneyCounterparty.Unknown -> Unit
            }
        }

        internal fun stageOf(leg: WalletMovingLeg): MoneyLegStage = when (leg) {
            WalletMovingLeg.RESERVED -> MoneyLegStage.RESERVED
            WalletMovingLeg.IN_TRANSIT -> MoneyLegStage.IN_TRANSIT
            WalletMovingLeg.EXPECTED -> MoneyLegStage.EXPECTED
        }

        internal fun predictedLegs(prediction: MoneyPrediction, snapshot: WalletSnapshot): LegsByEnd {
            val out = LegsByEnd()
            val fromParty = counterparty(prediction.from, snapshot)
            val toParty = counterparty(prediction.to, snapshot)
            val id = prediction.operationId ?: prediction.requestId
            val departing = MoneyLeg(id, prediction.requestId, prediction.kind, MoneyDirection.OUTGOING, MoneyLegStage.RESERVED, prediction.amountMicro, toParty, predicted = true)
            val arriving = MoneyLeg(id, prediction.requestId, prediction.kind, MoneyDirection.INCOMING, MoneyLegStage.RESERVED, prediction.amountMicro, fromParty, predicted = true)
            place(departing, fromParty, out)
            place(arriving, toParty, out)
            if (toParty.crossesWallet && !fromParty.crossesWallet) out.total.departing += departing
            if (fromParty.crossesWallet && !toParty.crossesWallet) out.total.arriving += arriving
            return out
        }

        internal sealed class Revaluation {
            class Success(val micro: String) : Revaluation()
            class Failure(val reason: String) : Revaluation()
        }

        /**
         * A venue frame is used as is only when it is newer than the account's
         * figure and the frame lists no settlement toward the account.
         */
        internal fun revaluation(revaluation: MoneyRevaluation, account: WalletTradingAccount, figure: MoneyFigure): Revaluation {
            account.heldByOperation?.let { return Revaluation.Failure("account held by $it") }
            if (figure.arriving.any { it.stage != MoneyLegStage.EXPECTED }) return Revaluation.Failure("settlement pending toward the account")
            val figureTime = account.asOf?.let(::parseInstant)
                ?: return if (account.balances == null) Revaluation.Failure("no figure to revalue") else Revaluation.Success(revaluation.equityMicro)
            if (revaluation.observedAt.isBefore(figureTime)) return Revaluation.Failure("venue frame older than the figure")
            return Revaluation.Success(revaluation.equityMicro)
        }

        /** `cash.confirmed + moving(in transit) + Σ accounts`, for the diagnostic only. */
        internal fun composedTotal(snapshot: WalletSnapshot): String? {
            val terms = mutableListOf(snapshot.cash.balances.confirmedMicro)
            for (account in snapshot.accounts) {
                if (account.lifecycle == WalletTradingAccountState.ARCHIVED.wire) continue
                terms += (account.balances ?: return null).totalMicro
            }
            terms += snapshot.moving.filter { it.typedLeg == WalletMovingLeg.IN_TRANSIT }.map { it.amountMicro }
            return Micro.sum(terms)
        }

        internal fun change(previous: MoneyFigure?, figure: MoneyFigure, snapshot: WalletSnapshot, previousMoney: WalletMoney, sequence: Long, account: WalletTradingAccount?): MoneyChange? {
            val before = previous?.settledMicro
            val after = figure.settledMicro
            val delta = if (before != null && after != null) Micro.difference(after, before) else null
            if (delta == null) {
                val cause = operationThatMoved(figure, previous, snapshot, previousMoney) ?: return null
                return MoneyChange(cause, "0", sequence)
            }
            operationThatMoved(figure, previous, snapshot, previousMoney)?.let { return MoneyChange(it, delta, sequence) }
            if (delta == "0") return null
            if (account != null && account.balances?.typedSource == WalletNativeBalanceSource.HYPERLIQUID_WS) {
                return MoneyChange(MoneyChange.Cause.Venue, delta, sequence)
            }
            return null
        }

        /** An operation that touches the figure and changed between the frames. */
        internal fun operationThatMoved(figure: MoneyFigure, previous: MoneyFigure?, snapshot: WalletSnapshot, previousMoney: WalletMoney): MoneyChange.Cause? {
            val nowLegs = (figure.arriving + figure.departing).associateBy { it.id }
            val beforeLegs = ((previous?.arriving ?: emptyList()) + (previous?.departing ?: emptyList())).associateBy { it.id }
            val touched = mutableListOf<Pair<String, String>>()
            for ((id, leg) in nowLegs) if (beforeLegs[id]?.stage != leg.stage) touched += leg.operationId to leg.kind
            for ((id, leg) in beforeLegs) if (nowLegs[id] == null) touched += leg.operationId to leg.kind
            val before = previousMoney.operations.associateBy { it.operationId }
            for (op in snapshot.operations) {
                if (!touches(op, figure, snapshot)) continue
                val old = before[op.operationId]
                if (old == null || old.stage != op.stage || old.status != op.status || old.creditBooked != op.creditBooked || old.debitBooked != op.debitBooked) {
                    touched += op.operationId to op.kind
                }
            }
            if (figure.kind is MoneyFigureKind.Cash || figure.kind is MoneyFigureKind.Total) {
                val beforeCash = previousMoney.snapshot.cash.operations.associateBy { it.id }
                for (op in snapshot.cash.operations) {
                    if (beforeCash[op.id]?.state != op.state && (op.kind == "send" || figure.kind is MoneyFigureKind.Cash)) touched += op.id to op.kind
                }
            }
            val (id, kind) = touched.firstOrNull() ?: return null
            return MoneyChange.Cause.Operation(id, kind)
        }

        internal fun touches(op: WalletFundingOperation, figure: MoneyFigure, snapshot: WalletSnapshot): Boolean {
            val ends = listOf(counterparty(op.from, snapshot), counterparty(op.to, snapshot))
            return when (val kind = figure.kind) {
                is MoneyFigureKind.Cash -> ends.any { it is MoneyCounterparty.Cash }
                is MoneyFigureKind.Account -> ends.any { it is MoneyCounterparty.Account && it.arcaId == kind.arcaId }
                is MoneyFigureKind.Total -> ends.any { it.crossesWallet }
            }
        }

        private fun parseInstant(raw: String): Instant? = try { Instant.parse(raw) } catch (_: DateTimeParseException) { null }
    }
}

/**
 * Integer arithmetic on micro-USDC strings for captions and diagnostics.
 * A figure is never produced here; only differences and caption totals.
 */
public object Micro {
    public fun parse(raw: String): Long? = raw.trim().takeIf { it.isNotEmpty() }?.toLongOrNull()

    public fun sum(values: List<String>): String = values.sumOf { parse(it) ?: 0L }.toString()

    /** `a - b` as a signed integer string, null when either is not an integer. */
    public fun difference(a: String, b: String): String? {
        val x = parse(a) ?: return null
        val y = parse(b) ?: return null
        return (x - y).toString()
    }

    /** A decimal USD string ("6.49", "-0.5") as micro-USDC, truncated toward zero past six decimals; null when not a decimal. */
    public fun fromDecimal(raw: String): String? {
        var text = raw.trim()
        if (text.isEmpty()) return null
        var negative = false
        if (text.startsWith("-")) { negative = true; text = text.drop(1) } else if (text.startsWith("+")) text = text.drop(1)
        val parts = text.split(".")
        if (parts.size > 2) return null
        val whole = parts[0]
        if (!(whole.isEmpty() || whole.all { it.isDigit() })) return null
        val fraction = if (parts.size == 2) parts[1] else ""
        if (!fraction.all { it.isDigit() }) return null
        val padded = (fraction + "000000").take(6)
        val wholeValue = (if (whole.isEmpty()) "0" else whole).toLongOrNull() ?: return null
        val fractionValue = padded.toLongOrNull() ?: return null
        val micro = wholeValue * 1_000_000 + fractionValue
        return (if (negative) -micro else micro).toString()
    }
}
