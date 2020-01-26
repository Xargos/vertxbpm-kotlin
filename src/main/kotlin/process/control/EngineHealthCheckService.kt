package process.control

import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.EventBus
import io.vertx.ext.web.RoutingContext
import process.engine.EngineId
import process.engine.Repository

class EngineHealthCheckService(private val repository: Repository) {

    private val healthyEngineIds: MutableMap<EngineId, String> = mutableMapOf()
    private val deadEngineIds: MutableSet<EngineId> = mutableSetOf()
    private val deliveryOptions = DeliveryOptions().setSendTimeout(2000)

    fun registerEngine(nodeId: NodeId, engineId: EngineId, deploymentId: String): Future<Void> {
        healthyEngineIds[engineId] = deploymentId
        return repository.assignEngineToNode(nodeId, engineId)
    }

    fun getHealthyEngineIds(routingContext: RoutingContext) {
        routingContext.response().end(healthyEngineIds.toString())
    }

    fun startHealthChecks(
        nodeId: NodeId,
        vertx: Vertx,
        engineNo: Int,
        engineService: EngineService
    ) {
//        println("startHealthChecks")
        vertx.setTimer(5000) {
            //            println("Doing health check")
            CompositeFuture
                .join(healthyEngineIds.map { runHealthCheck(it.key, vertx.eventBus()) })
                .onFailure {
                    cleanUpDeadEngines(vertx, nodeId)
                        .compose { engineService.startEngines(vertx, engineNo - healthyEngineIds.size) }
                        .compose { this.resumeStoppedProcesses(engineService, vertx.eventBus()) }
                        .setHandler {
                            deadEngineIds.clear()
                        }
                }
                .onComplete { startHealthChecks(nodeId, vertx, engineNo, engineService) }
        }
    }

    private fun resumeStoppedProcesses(engineService: EngineService, eventBus: EventBus): Future<Void> {
        return repository.getActiveProcesses(deadEngineIds)
            .compose { engineService.restartProcesses(it, eventBus) }
    }

    private fun cleanUpDeadEngines(vertx: Vertx, nodeId: NodeId): Future<Void> {
        println("cleanUpDeadEngines")
        val promise = Promise.promise<Void>()
        CompositeFuture
            .join(undeployDeadEngines(vertx))
            .compose { this.repository.removeDeadEnginesFromCache(nodeId, deadEngineIds) }
            .onFailure { it.printStackTrace() }
            .onComplete {
                println("cleanUpDeadEngines done")
                promise.complete()
            }
        return promise.future()
    }

    private fun undeployDeadEngines(vertx: Vertx): List<Future<Void>> {
        return deadEngineIds.map {
            val undeployPromise = Promise.promise<Void>()
            val deploymentId = healthyEngineIds.remove(it)
            if (vertx.deploymentIDs().contains(deploymentId)) {
                vertx.undeploy(deploymentId, undeployPromise::handle)
            } else {
                undeployPromise.complete()
            }
            undeployPromise.future()
        }
    }

    private fun runHealthCheck(engineId: EngineId, eventBus: EventBus): Future<Void> {
        val healthCheck = Promise.promise<Void>()
        eventBus.request<String>("{$engineId}_healthcheck", "", deliveryOptions)
        {
            if (it.failed()) {
                println("health check failed")
                deadEngineIds.add(engineId)
                healthCheck.fail("")
            } else {
//                println("health check success")
                healthCheck.complete()
            }

        }
//            .onSuccess {
//                println("health check success")
//                healthCheck.complete()
//            }
//            .onFailure {
//                println("health check failed")
//                deadEngineIds.add(engineId)
//                healthCheck.complete()
//            }
        return healthCheck.future()
    }
}