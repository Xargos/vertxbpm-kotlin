package process.engine

import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.ext.web.RoutingContext

data class Statistics(val processCount: Int, val activeProcessCount: Int, val finishedProcessesCount: Int)

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
//        return repository.getOrCreateProcess<T>(processId)
//            .compose {
//                val flowContext =
//                    (it ?: throw RuntimeException("Unknown process: $processId")) as FlowContext<*>
//                Future.succeededFuture(flowContext.currentStep.stepName)
//            }
        return Future.succeededFuture<StepName>()
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

    fun getStatistics(): Future<Statistics> {
        val promise = Promise.promise<Statistics>()
        try {
            val allProcesses = repository.retrieveAllProcesses()

            val statistics = Statistics(
                processCount = allProcesses.count(),
                activeProcessCount = allProcesses.filter { !it.ended }.count(),
                finishedProcessesCount = allProcesses.filter { it.ended }.count()
            )

            promise.complete(statistics)
        } catch (e: Exception) {
            e.printStackTrace()
            promise.fail(e)
        }
        return promise.future()
    }
}