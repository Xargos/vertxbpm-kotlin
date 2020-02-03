package process.engine

import io.vertx.core.Future
import java.io.Serializable

data class StepContext<T>(val stepName: StepName, val data: T) : Serializable

data class FlowContext<T>(
    val workflowName: String,
    val processId: ProcessId,
    val currentStep: StepContext<T>,
    val history: List<StepContext<T>>,
    val ended: Boolean = false,
    val exception: Throwable? = null
) : Serializable

data class StepName(val name: String) : Serializable

data class Workflow<T>(
    val name: String,
    val startNode: StepName,
    val steps: Map<StepName, Step<T>>,
    val decodeData: (data: String?) -> T
) : Serializable

sealed class Step<T> {
    abstract val name: StepName
    abstract val exec: (data: T) -> Future<T>

    class Simple<T>(
        override val name: StepName,
        val next: StepName,
        override val exec: (data: T) -> Future<T>
    ) : Step<T>()

    class Choice<T>(
        override val name: StepName,
        val next: Set<StepName>,
        override val exec: (data: T) -> Future<T> = { Future.succeededFuture(it) },
        val choose: (data: T) -> Future<StepName>
    ) : Step<T>()

    class End<T>(
        override val name: StepName,
        override val exec: (data: T) -> Future<T>
    ) : Step<T>()
}
