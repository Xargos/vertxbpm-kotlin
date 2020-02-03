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
                    standardStep(step, data, flowContext, steps)
                }
                is Step.Choice -> {
                    choiceStep(step, data, flowContext, steps)
                }
                is Step.NoSave -> {
                    noSaveStep(step, data, flowContext, steps)
                }
            }
        } catch (e: Exception) {
            Future.failedFuture<Void>(e)
        }
    }

    private fun <T> noSaveStep(
        step: Step.NoSave<T>,
        data: T,
        flowContext: FlowContext<T>,
        steps: Map<StepName, Step<T>>
    ): Future<Void> {
        val nextStep = StepContext(step.next, data)
        return this.execStep(
            steps,
            flowContext.copy(currentStep = nextStep, history = flowContext.history.plus(nextStep))
        )
    }

    private fun <T> choiceStep(
        step: Step.Choice<T>,
        data: T,
        flowContext: FlowContext<T>,
        steps: Map<StepName, Step<T>>
    ): Future<Void> {
        return step.choose(data)
            .compose {
                val nextStep = StepContext(it, data)
                val newfc =
                    flowContext.copy(currentStep = nextStep, history = flowContext.history.plus(nextStep))
                repository.saveProcess(newfc)
                    .compose { this.execStep(steps, newfc) }
            }
    }

    private fun <T> standardStep(
        step: Step.Standard<T>,
        data: T,
        flowContext: FlowContext<T>,
        steps: Map<StepName, Step<T>>
    ): Future<Void> {
        val nextStep = StepContext(step.next, data)
        val newfc = flowContext.copy(currentStep = nextStep, history = flowContext.history.plus(nextStep))
        return repository.saveProcess(newfc)
            .compose { this.execStep(steps, newfc) }
    }
}
