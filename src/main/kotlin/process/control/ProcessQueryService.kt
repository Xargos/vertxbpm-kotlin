package process.control

import io.vertx.core.Future
import io.vertx.ext.web.RoutingContext
import process.engine.FlowContext
import process.engine.ProcessId
import process.engine.Repository
import process.engine.StepName

class ProcessQueryService(
    private val repository: Repository
) {

    fun getProcess(routingContext: RoutingContext) {
        try {

            getProcessStep<Any>(ProcessId(routingContext.pathParam("processId")))
                .onSuccess {
                    val response = routingContext.response()
                    response.putHeader("content-type", "text/plain")
                    response.end(it.name)
                }
                .onFailure {
                    routingContext.fail(it)
                }
        } catch (e: Exception) {
            e.printStackTrace()
            routingContext.fail(e)
        }
    }

    private fun <T> getProcessStep(processId: ProcessId): Future<StepName> {
        return repository.retrieveProcess<T>(processId)
            .compose {
                val flowContext =
                    (it ?: throw RuntimeException("Unknown process: $processId")) as FlowContext<*>
                Future.succeededFuture(flowContext.currentStep.stepName)
            }
    }

    fun getProcesses(routingContext: RoutingContext) {
        try {
            val retrieveAllFlows = repository.retrieveAllProcesses()
            // This handler will be called for every request
            val response = routingContext.response()
            response.putHeader("content-type", "text/plain")

            // Write to the response and end it
            response.end("$retrieveAllFlows")
        } catch (e: Exception) {
            e.printStackTrace()
            routingContext.fail(e)
        }
    }
}