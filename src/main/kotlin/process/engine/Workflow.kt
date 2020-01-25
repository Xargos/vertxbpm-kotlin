package process.engine

import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.ext.web.RoutingContext
import java.io.Serializable

data class StepContext<T>(val stepName: StepName, val data: T) : Serializable

data class FlowContext<T>(
    val workflowName: String,
    val processId: ProcessId,
    val currentStep: StepContext<T>,
    val history: List<StepContext<T>>
) : Serializable

data class StepName(val name: String) : Serializable

interface Workflow<T> : Serializable {
    fun name(): String
    fun startNode(): StepName
    fun steps(vertx: Vertx): Map<StepName, Step<T>>
    fun decodeData(data: String?): T
}

sealed class Step<T> {
    abstract val name: StepName
    abstract val exec: (data: T) -> Future<T>

    class Start<T>(
        override val name: StepName,
        val next: StepName,
        override val exec: (data: T) -> Future<T>
    ) : Step<T>()

    class Simple<T>(
        override val name: StepName,
        val next: StepName,
        override val exec: (data: T) -> Future<T>
    ) : Step<T>()

    class End<T>(
        override val name: StepName,
        override val exec: (data: T) -> Future<T>
    ) : Step<T>()
}
