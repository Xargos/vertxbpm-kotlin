package process.control

import de.huxhorn.sulky.ulid.ULID
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.EventBus
import io.vertx.ext.web.RoutingContext
import process.engine.*


class EngineService(
    private val engineHealthCheckService: EngineHealthCheckService,
    private val workflowEngineFactory: WorkflowEngineFactory,
    private val workflowStore: WorkflowStore,
    private val ulid: ULID
) {

    fun undeployEngine(routingContext: RoutingContext, vertx: Vertx) {
        println("undeployEngine")
        val deploymentId = routingContext.pathParam("deploymentId")
        vertx.undeploy(deploymentId) {
            println("undeployEngine done")
            println(it)
            if (it.failed()) {
                routingContext.fail(it.cause())
            } else {
                routingContext.response().end()
            }
        }
//            .onFailure { routingContext.fail(it) }
//            .onSuccess { routingContext.response().end() }
    }

    fun startEngines(vertx: Vertx, engineNo: Int, nodeId: NodeId): Future<Void> {
        println("startEngines")
        val enginesStart = Promise.promise<Void>()
        CompositeFuture.join((1..engineNo).map { startEngine(vertx, nodeId) })
            .onSuccess {
                println("all deployed")
                enginesStart.complete()
            }
            .onFailure {
                it.printStackTrace()
                enginesStart.fail(it)
            }

        return enginesStart.future()
    }

    private fun startEngine(vertx: Vertx, nodeId: NodeId): Future<String> {
        val engineStart = Promise.promise<String>()
        println("Deploying Engine: $engineStart")
        val engineId = EngineId(nodeId.value + "_" + ulid.nextULID())
        val workflowEngine = workflowEngineFactory.buildEngine(engineId)
        val engineVerticle = EngineVerticle(
            engineId,
            workflowEngine,
            workflowStore,
            startProcessTopic = nodeId.value + "_engine_startprocess"
        )
        vertx.deployVerticle(engineVerticle) {
            if (it.failed()) {
                println("Engine start failed")
                //                workflowEngine.pickUpExistingJobs()
                engineStart.fail(it.cause())
            } else {
//                println("Engine: $engineId is Up $engineStart")
                engineHealthCheckService.registerEngine(nodeId, engineId, it.result())
                engineStart.complete("")
                println("Engine: $engineId is Up ${engineStart.future()}")
            }
        }
//            .onSuccess {
////                println("Engine: $engineId is Up $engineStart")
//                engineHealthCheckService.registerEngine(engineId, it)
//                engineStart.complete("")
//                println("Engine: $engineId is Up ${engineStart.future()}")
//            }
//            .onFailure {
//                println("Engine start failed")
//                //                workflowEngine.pickUpExistingJobs()
//                engineStart.fail(it)
//            }
        return engineStart.future()
    }

    fun startProcess(
        nodeId: NodeId,
        workflowName: String,
        body: String,
        eventBus: EventBus,
        processId: String = ulid.nextULID()
    ): Future<String> {
        val promise = Promise.promise<String>()
        val deliveryOptions = DeliveryOptions()
        deliveryOptions.sendTimeout = 2000
        deliveryOptions.addHeader("workflowName", workflowName)
        deliveryOptions.addHeader("processId", processId)
        eventBus.request<String>(
            nodeId.value + "_engine_startprocess",
            body,
            deliveryOptions
        ) {
            if (it.failed()) {
                promise.fail(it.cause())
            } else {
                promise.complete(it.result().body())
            }
        }
        return promise.future()
//                .onSuccess {
//                    val response = routingContext.response()
//                    response.putHeader("content-type", "text/plain")
//                    response.end("Execute workflow! ProcessId: ${it.body()}")
//                }
//                .onFailure {
//                    routingContext.fail(it)
//                }
    }

    fun restartProcesses(
        flowContexts: List<FlowContext<Any>>,
        eventBus: EventBus,
        nodeId: NodeId
    ): Future<Void> {
        val promise = Promise.promise<Void>()
        CompositeFuture.join(flowContexts.map {
            startProcess(
                nodeId,
                it.workflowName,
                "",
                eventBus,
                it.processId.value
            )
        })
            .onSuccess { promise.complete() }
            .onFailure { promise.fail(it) }
        return promise.future()
    }
}