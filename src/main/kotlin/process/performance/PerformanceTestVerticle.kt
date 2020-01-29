package process.performance

import io.vertx.core.AbstractVerticle
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.eventbus.EventBus
import process.control.EngineService
import process.control.NodeId
import process.control.ProcessQueryService
import process.engine.Repository

class PerformanceTestVerticle(
    private val engineService: EngineService,
    private val processQueryService: ProcessQueryService,
    private val repository: Repository
) : AbstractVerticle() {

    private val nodeId = NodeId("TEST")

    override fun start(startPromise: Promise<Void>) {
//        nodeId = NodeId((vertx as VertxImpl).nodeID)
        CompositeFuture.join(repository.init(vertx), engineService.startEngines(vertx, 1, nodeId))
            .onSuccess {
                ////                engineHealthCheckService.startHealthChecks(nodeId, vertx, config.engineNo, engineService)
//                nodeSynchronizationService.subscribeNodeExistence(vertx)
//                nodeSynchronizationService.listenToWaitingProcesses(vertx, engineService, nodeId)
                startPromise.complete()
            }
            .onFailure {
                it.printStackTrace()
                startPromise.fail(it)
            }
    }

    fun startProcess(workflowName: String, body: String, eventBus: EventBus): Future<Void> {
        val promise = Promise.promise<Void>()
        engineService.startProcess(nodeId, workflowName, body ?: "", eventBus)
            .setHandler { it1 ->
                if (it1.failed()) {
                    promise.fail(it1.cause())
                } else {
                    promise.complete()
                }
            }
        return promise.future()
    }

    fun getProcessCount(): Future<Int> {
        return processQueryService.getProcessesCount()
    }

    fun getActiveProcessesCount(): Future<Int> {
        return processQueryService.getActiveProcessesCount()
    }

    fun getTotalFinishedProcesses(): Int {
        return processQueryService.getTotalFinishedProcesses()
    }
}