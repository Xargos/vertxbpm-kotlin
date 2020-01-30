package process.engine

import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.ext.web.RoutingContext
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch

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

    fun getProcessesCount(): Future<Int> {
        val promise = Promise.promise<Int>()
        GlobalScope.launch {
            try {
                val count = repository.retrieveAllProcesses().count()
                promise.complete(count)
            } catch (e: Exception) {
                e.printStackTrace()
                promise.fail(e)
            }
        }
        return promise.future()
    }

    fun getActiveProcessesCount(): Future<Int> {
        val promise = Promise.promise<Int>()
        try {
            val count = repository.retrieveAllProcesses()
                .filter { !it.ended }
                .count()
            promise.complete(count)
        } catch (e: Exception) {
            e.printStackTrace()
            promise.fail(e)
        }
        return promise.future()
    }

    fun getTotalFinishedProcesses(): Int {
        return repository.retrieveAllProcesses()
            .filter { it.ended }
            .count()
    }
}