package process.engine

import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.impl.VertxImpl
import io.vertx.core.spi.cluster.NodeListener


class NodeSynchronizationService(private val repository: Repository) {
    class HealthCheck(private val repository: Repository) : NodeListener {

        override fun nodeAdded(nodeID: String?) {
            println("node added $nodeID")
        }

        override fun nodeLeft(nodeID: String) {
            repository.moveDeadNodeProcessesToWaitQueueAndCleanup(NodeId(nodeID))
        }
    }

    fun subscribeNodeExistence(vertx: Vertx) {
        val v: VertxImpl = vertx as VertxImpl
        v.clusterManager.nodeListener(
            HealthCheck(
                repository
            )
        )
    }

    fun listenToWaitingProcesses(vertx: Vertx, engineService: EngineService, nodeId: NodeId) {
        vertx.setTimer(10_000) {
            vertx.executeBlocking<Void>(
                { promise ->
                    repository.getAndExecuteWaitingProcess {
                        engineService.startProcess(it.workflowName, "", vertx, it.processId)
                            .compose { Future.succeededFuture<Void>() }
                    }
                        .setHandler(promise::handle)
                },
                true
            )
            { listenToWaitingProcesses(vertx, engineService, nodeId) }
        }
    }

}