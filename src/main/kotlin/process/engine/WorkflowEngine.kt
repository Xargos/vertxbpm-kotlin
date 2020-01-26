package process.engine

import io.vertx.core.Future
import io.vertx.core.Vertx
import java.io.Serializable

data class ProcessId(val value: String) : Serializable

class WorkflowEngineFactory(private val repository: Repository) {
    fun buildEngine(engineId: EngineId): WorkflowEngine {
        return WorkflowEngine(repository, engineId)
    }
}

class WorkflowEngine(
    private val repository: Repository,
    private val engineId: EngineId
) {

    fun <T> start(
        workflow: Workflow<T>,
        vertx: Vertx,
        inputData: T,
        processId: ProcessId
    ) {
        val firstStep = StepContext(workflow.startNode(), inputData)
        val flowContext = FlowContext(workflow.name(), processId, firstStep, listOf(firstStep))
        repository.assignProcessToEngine(engineId, processId)
            .compose { repository.retrieveProcess<T>(flowContext.processId) }
            .compose { this.execStep(workflow.steps(vertx), (it ?: flowContext)) }
            .compose { repository.removeProcessFromEngine(engineId, processId) }
            .onFailure { it.printStackTrace() }
            .onSuccess { println("Success") }
    }

    private fun <T> execStep(
        steps: Map<StepName, Step<T>>,
        flowContext: FlowContext<T>
    ): Future<Void> {
        return repository.saveProcess(flowContext, flowContext.processId)
            .compose {
                try {
                    val step = steps[flowContext.currentStep.stepName]
                    step
                        ?.exec
                        ?.invoke(flowContext.currentStep.data)
                        ?.compose { next(steps, step, flowContext, it) }
                        ?: Future.failedFuture(RuntimeException("No step"))
                } catch (e: Exception) {
                    Future.failedFuture<Void>(e)
                }
            }
    }

    private fun <T> next(
        steps: Map<StepName, Step<T>>,
        step: Step<T>,
        flowContext: FlowContext<T>,
        data: T
    ): Future<Void> {
        return when (step) {
            is Step.End -> Future.succeededFuture()
            is Step.Start -> {
                val nextStep = StepContext(step.next, data)
                this.execStep(
                    steps,
                    flowContext.copy(currentStep = nextStep, history = flowContext.history.plus(nextStep))
                )
            }
            is Step.Simple -> {
                val nextStep = StepContext(step.next, data)
                this.execStep(
                    steps,
                    flowContext.copy(currentStep = nextStep, history = flowContext.history.plus(nextStep))
                )
            }
        }
    }
}
