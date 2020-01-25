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

    val healthyEngineIds: MutableMap<EngineId, String> = mutableMapOf()
    val deadEngineIds: MutableSet<EngineId> = mutableSetOf()

    fun registerEngine(engineId: EngineId, deploymentId: String) {
        healthyEngineIds[engineId] = deploymentId
    }

    fun getHealthyEngineIds(routingContext: RoutingContext) {
        routingContext.response().end(healthyEngineIds.toString())
    }

    fun startHealthChecks(
        vertx: Vertx,
        engineNo: Int,
        engineService: EngineService
    ) {
        println("startHealthChecks")
        vertx.setTimer(5000) {
            println("Doing health check")
            CompositeFuture
                .join(healthyEngineIds.map { runHealthCheck(it.key, vertx.eventBus()) })
                .onFailure {
                    cleanUpDeadEngines(vertx)
                        .compose { engineService.startEngines(vertx, engineNo - healthyEngineIds.size) }
                        .compose { this.resumeStoppedProcesses(engineService, vertx.eventBus()) }
                        .setHandler {
                            deadEngineIds.clear()
                        }
                }
                .onComplete { startHealthChecks(vertx, engineNo, engineService) }
        }
    }

    private fun resumeStoppedProcesses(engineService: EngineService, eventBus: EventBus): Future<Void> {
        return repository.getActiveProcesses(deadEngineIds)
            .compose { engineService.restartProcesses(it.map { it1 -> it1.workflowName }, eventBus) }
    }

    private fun cleanUpDeadEngines(vertx: Vertx): Future<Void> {
        println("cleanUpDeadEngines")
        val promise = Promise.promise<Void>()
        CompositeFuture
            .join(deadEngineIds.map {
                val undeployPromise = Promise.promise<Void>()
                val deploymentId = healthyEngineIds.remove(it)
                if (vertx.deploymentIDs().contains(deploymentId)) {
                    vertx.undeploy(deploymentId, undeployPromise::handle)
                } else {
                    undeployPromise.complete()
                }
                undeployPromise.future()
            })
            .onFailure { it.printStackTrace() }
            .onComplete {
                println("cleanUpDeadEngines done")
                promise.complete()
            }
        return promise.future()
    }

    private fun runHealthCheck(engineId: EngineId, eventBus: EventBus): Future<Void> {
        val healthCheck = Promise.promise<Void>()
        val deliveryOptions = DeliveryOptions()
        deliveryOptions.sendTimeout = 1000
        eventBus.request<String>("{$engineId}_healthcheck", "", deliveryOptions)
        {
            if (it.failed()) {
                println("health check failed")
                deadEngineIds.add(engineId)
                healthCheck.fail("")
            } else {
                println("health check success")
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