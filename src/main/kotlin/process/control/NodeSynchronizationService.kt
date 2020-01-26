package process.control

import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.EventBus
import process.engine.Repository

class NodeSynchronizationService(
    private val repository: Repository,
    private val nodeId: NodeId
) {

    private val nodeHealthCheckAddress: String = "node_health_check_${nodeId.value}"
    private val deliveryOptions = DeliveryOptions().setSendTimeout(2000)

    fun subscribeNodeExistence(eventBus: EventBus) {
        eventBus.consumer<Void>(nodeHealthCheckAddress) {
            it.reply("")
        }
        repository.subscribeNodeExistence(nodeId)
    }

    fun runNodeHealthCheck(vertx: Vertx, engineService: EngineService) {
        val eventBus = vertx.eventBus()
        vertx.setTimer(5000) {
            println("runNodeHealthCheck")
            repository.getSubscribedNodes()
                .compose { nodeIds ->
                    println(nodeIds)
                    //                    val promise = Promise.promise<Void>()
                    CompositeFuture.join(sendHealthChecks(nodeIds, eventBus))
                        .map {
                            it.result()
                                .list<NodeId?>()
                                .filterNotNull()
                                .toSet()
                        }
                        .compose { this.runNodeCleanUp(it, engineService, eventBus) }
//                        .onComplete {
//
//                        }
//                    promise.future()
                }
                .onComplete { runNodeHealthCheck(vertx, engineService) }
        }
    }

    private fun runNodeCleanUp(deadNodes: Set<NodeId>, engineService: EngineService, eventBus: EventBus): Future<Void> {
        if(deadNodes.isEmpty()) {
            return Future.succeededFuture()
        }
        println("runNodeCleanUp")
        return repository.getProcessesOfDeadNodes(deadNodes)
            .compose { engineService.restartProcesses(it, eventBus) }
            .compose { repository.removeNodesExistence(deadNodes) }
    }

    private fun sendHealthChecks(
        nodeIds: Set<NodeId>,
        eventBus: EventBus
    ): List<Future<NodeId?>> {
        return nodeIds
            .map { nodeId ->
                val promise = Promise.promise<NodeId?>()
                eventBus.request<String>("node_health_check_${nodeId.value}", "", deliveryOptions) {
                    if (it.failed()) {
                        println(it.cause())
                        promise.complete(nodeId)
                    } else {
                        promise.complete()
                    }
                }
                promise.future()
            }
    }
}