package process.control

import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.impl.VertxImpl
import io.vertx.core.spi.cluster.NodeListener
import process.engine.Repository


class NodeSynchronizationService(private val repository: Repository) {
    class HealthCheck(
        private val repository: Repository,
        private val vertx: Vertx
    ) : NodeListener {

        override fun nodeAdded(nodeID: String?) {
            println("node added $nodeID")
        }

        override fun nodeLeft(nodeID: String) {
            repository.moveDeadNodeProcessesToWaitQueueAndCleanup(NodeId(nodeID))
        }
    }

    fun subscribeNodeExistence(vertx: Vertx) {
        val v: VertxImpl = vertx as VertxImpl
        v.clusterManager.nodeListener(HealthCheck(repository, vertx))
    }

    fun listenToWaitingProcesses(vertx: Vertx, engineService: EngineService, nodeId: NodeId) {
        vertx.setTimer(10_000) {
            vertx.executeBlocking<Void>(
                { promise ->
                    repository.getAndExecuteWaitingProcess {
                        engineService.startProcess(nodeId, it.workflowName, "", vertx.eventBus(), it.processId.value)
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