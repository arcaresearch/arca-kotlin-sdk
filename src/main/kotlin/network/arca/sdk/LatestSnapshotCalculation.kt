package network.arca.sdk

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch

/** One active calculation and one newest pending derived snapshot, never event history. */
internal class LatestSnapshotCalculation<Input, Output>(
    scope: CoroutineScope,
    calculate: suspend (Input) -> Output,
    publish: (Input, Output) -> Unit,
) {
    private val pending = Channel<Input>(Channel.CONFLATED)
    private val worker = scope.launch(Dispatchers.Default) {
        for (input in pending) {
            ensureActive()
            val output = calculate(input)
            ensureActive()
            publish(input, output)
        }
    }

    fun submit(input: Input) { pending.trySend(input) }
    fun stop() { pending.cancel(); worker.cancel() }
}
