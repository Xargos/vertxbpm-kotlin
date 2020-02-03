package process.engine

import io.vertx.core.Future
import io.vertx.core.Promise.promise
import java.io.Serializable

data class ProcessId(val value: String) : Serializable

class Engine(
    private val repository: Repository
) {

    fun <T> start(
        workflow: Workflow<T>,
        inputData: T,
        processId: ProcessId
    ): Future<Void> {
        val promise = promise<Void>()
        val firstStep = StepContext(workflow.startNode, inputData)
        val flowContext = FlowContext(workflow.name, processId, firstStep, listOf(firstStep))
        repository.startNewProcess(processId)
            .compose { repository.getOrCreateProcess(flowContext) }
            .compose {
                promise.complete()
                this.execStep(workflow.steps, (it ?: flowContext))
            }
            .onFailure {
                it.printStackTrace()
                repository.finishProcess(flowContext.copy(ended = true, exception = it))
                promise.tryFail(it)
            }
            .onComplete { repository.removeProcessFromEngine(processId) }
        return promise.future()
    }

    private fun <T> execStep(
        steps: Map<StepName, Step<T>>,
        flowContext: FlowContext<T>
    ): Future<Void> {
        return try {
            if (steps.containsKey(flowContext.currentStep.stepName)) {
                val step: Step<T> = steps.getValue(flowContext.currentStep.stepName)
                step.exec(flowContext.currentStep.data)
                    .compose { next(steps, step, flowContext, it) }
            } else {
                Future.failedFuture(RuntimeException("No step"))
            }
        } catch (e: Exception) {
            Future.failedFuture(e)
        }
    }

    private fun <T> next(
        steps: Map<StepName, Step<T>>,
        step: Step<T>,
        flowContext: FlowContext<T>,
        data: T
    ): Future<Void> {
        return try {
            when (step) {
                is Step.End -> {
                    repository.finishProcess(flowContext.copy(ended = true))
                }
                is Step.Standard -> {
                    repository.saveProcess(flowContext)
                        .compose {
                            val nextStep = StepContext(step.next, data)
                            this.execStep(
                                steps,
                                flowContext.copy(currentStep = nextStep, history = flowContext.history.plus(nextStep))
                            )
                        }
                }
                is Step.Choice -> {
                    repository.saveProcess(flowContext)
                        .compose { step.choose(data) }
                        .compose {
                            val nextStep = StepContext(it, data)
                            this.execStep(
                                steps,
                                flowContext.copy(currentStep = nextStep, history = flowContext.history.plus(nextStep))
                            )
                        }
                }
            }
        } catch (e: Exception) {
            Future.failedFuture<Void>(e)
        }
    }
}
