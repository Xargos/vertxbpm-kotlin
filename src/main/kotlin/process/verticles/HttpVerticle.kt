package process.verticles

import io.vertx.core.AbstractVerticle
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.eventbus.EventBus
import io.vertx.core.impl.VertxImpl
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import process.engine.EngineService
import process.engine.NodeSynchronizationService
import process.engine.ProcessQueryService
import java.io.Serializable

data class Config(val port: Int) : Serializable


class HttpVerticle(
    private val engineService: EngineService,
    private val processQueryService: ProcessQueryService,
    private val nodeSynchronizationService: NodeSynchronizationService,
    private val config: Config
) : AbstractVerticle() {

    private lateinit var nodeId: NodeId

    override fun start(startPromise: Promise<Void>) {
        nodeId = NodeId((vertx as VertxImpl).nodeID)
        CompositeFuture.join(startHttpServer(), engineService.startEngines(vertx, config.engineNo, nodeId))
            .onSuccess {
                nodeSynchronizationService.subscribeNodeExistence(vertx)
                nodeSynchronizationService.listenToWaitingProcesses(vertx, engineService, nodeId)
                startPromise.complete()
            }
            .onFailure {
                it.printStackTrace()
                startPromise.fail(it)
            }
    }

    private fun startHttpServer(): Future<Void> {
        val eventBus = vertx.eventBus()
        val server = vertx.createHttpServer()

        val router = Router.router(vertx)

        router.route("/workflow/:workflowName").handler { startProcess(it, eventBus) }

        router.route("/processes/:processId").handler(processQueryService::getProcess)
        router.route("/processes/count").handler { routingContext ->
            processQueryService.getProcessesCount()
                .onSuccess {
                    val response = routingContext.response()
                    response.putHeader("content-type", "text/plain")
                    response.end("$it")
                }
                .onFailure {
                    routingContext.fail(it.cause)
                }
        }
        router.route("/processes/activecount").handler { routingContext ->
            processQueryService.getActiveProcessesCount()
                .onSuccess {
                    val response = routingContext.response()
                    response.putHeader("content-type", "text/plain")
                    response.end("$it")
                }
                .onFailure {
                    routingContext.fail(it.cause)
                }
        }
        router.route("/processes/").handler(processQueryService::getProcesses)
        router.route("/deployments/").handler { it.response().end(vertx.deploymentIDs().toString()) }
        router.route("/engines/").handler { it.response().end(engineHealthCheckService.getHealthyEngineIds()) }
        router.route("/engines/deploy").handler { engineService.startEngines(vertx, 2, nodeId) }
        router.route("/engines/:deploymentId").handler { engineService.undeployEngine(it, vertx) }
        val serverStart = Promise.promise<Void>()
        server
            .requestHandler(router)
            .listen(config.port)
        serverStart.complete()
//            .onSuccess {
//                println("Server is Up")
//                //                workflowEngine.pickUpExistingJobs()
//                serverStart.complete()
//            }
//            .onFailure {
//                println("Server start failed")
//                //                workflowEngine.pickUpExistingJobs()
//                serverStart.fail(it)
//            }
        return serverStart.future()
    }

    private fun startProcess(it: RoutingContext, eventBus: EventBus) {
        val workflowName = it.pathParam("workflowName")
        engineService.startProcess(nodeId, workflowName, it.bodyAsString ?: "", eventBus)
            .setHandler { it1 ->
                if (it1.failed()) {
                    it1.cause().printStackTrace()
                    it.fail(it1.cause())
                } else {
                    val response = it.response()
                    response.putHeader("content-type", "text/plain")
                    response.end("Execute workflow! ProcessId: ${it1.result()}")
                }
            }
    }
}