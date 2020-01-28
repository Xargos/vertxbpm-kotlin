package process.engine

import io.vertx.core.Future
import io.vertx.core.Promise.promise
import io.vertx.core.Vertx
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
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
    ): Future<Void> {
        val promise = promise<Void>()
        val firstStep = StepContext(workflow.startNode(), inputData)
        val flowContext = FlowContext(workflow.name(), processId, firstStep, listOf(firstStep))
        repository.startNewProcess(processId)
            .compose { repository.getOrCreateProcess(flowContext) }
            .compose {
                promise.complete()
                this.execStep(workflow.steps(vertx), (it ?: flowContext))
                    .compose { repository.removeProcessFromEngine(engineId, processId) }
            }
            .onFailure {
                it.printStackTrace()
                promise.fail(it)
            }
//            .onSuccess { println("Success") }
        return promise.future()
    }

    private fun <T> execStep(
        steps: Map<StepName, Step<T>>,
        flowContext: FlowContext<T>
    ): Future<Void> {
        return try {
            if (steps.containsKey(flowContext.currentStep.stepName)) {
                val step: Step<T> = steps.getValue(flowContext.currentStep.stepName)
                step.exec.invoke(flowContext.currentStep.data)
                    .compose { next(steps, step, flowContext, it) }
            } else {
                Future.failedFuture(RuntimeException("No step"))
            }
        } catch (e: Exception) {
            Future.failedFuture<Void>(e)
        }
    }

    private fun <T> next(
        steps: Map<StepName, Step<T>>,
        step: Step<T>,
        flowContext: FlowContext<T>,
        data: T
    ): Future<Void> {
        return when (step) {
            is Step.End -> {
                repository.saveProcess(flowContext.copy(ended = true))
            }
            is Step.Start -> {
                repository.saveProcess(flowContext)
                    .compose {
                        val nextStep = StepContext(step.next, data)
                        this.execStep(
                            steps,
                            flowContext.copy(currentStep = nextStep, history = flowContext.history.plus(nextStep))
                        )
                    }
            }
            is Step.Simple -> {
                repository.saveProcess(flowContext)
                    .compose {
                        val nextStep = StepContext(step.next, data)
                        this.execStep(
                            steps,
                            flowContext.copy(currentStep = nextStep, history = flowContext.history.plus(nextStep))
                        )
                    }
            }
        }
    }
}
