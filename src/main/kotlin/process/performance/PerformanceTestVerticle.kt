package process.performance

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.eventbus.EventBus
import io.vertx.core.impl.VertxImpl
import process.control.*

class PerformanceTestVerticle(
    private val engineHealthCheckService: EngineHealthCheckService,
    private val engineService: EngineService,
    private val processQueryService: ProcessQueryService,
    private val nodeSynchronizationService: NodeSynchronizationService,
    private val config: Config
) : AbstractVerticle() {

    private lateinit var nodeId: NodeId

    override fun start(startPromise: Promise<Void>) {
        nodeId = NodeId((vertx as VertxImpl).nodeID)
        engineService.startEngines(vertx, config.engineNo, nodeId)
            .onSuccess {
//                engineHealthCheckService.startHealthChecks(nodeId, vertx, config.engineNo, engineService)
                nodeSynchronizationService.subscribeNodeExistence(vertx)
                nodeSynchronizationService.listenToWaitingProcesses(vertx, engineService, nodeId)
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

    fun getProcessCount():Future<Int> {
        return processQueryService.getProcessesCount()
    }

    fun getActiveProcessesCount():Future<Int> {
        return processQueryService.getActiveProcessesCount()
    }
}