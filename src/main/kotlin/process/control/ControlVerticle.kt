package process.control

import io.vertx.core.AbstractVerticle
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.eventbus.EventBus
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import java.io.Serializable


data class NodeId(val value: String) : Serializable

data class Config(val engineNo: Int) : Serializable

class ControlVerticle(
    private val engineHealthCheckService: EngineHealthCheckService,
    private val engineService: EngineService,
    private val processQueryService: ProcessQueryService,
    private val config: Config
) : AbstractVerticle() {
    override fun start(startPromise: Promise<Void>) {
        CompositeFuture.join(startHttpServer(), engineService.startEngines(vertx, config.engineNo))
            .onSuccess {
                engineHealthCheckService.startHealthChecks(vertx, config.engineNo, engineService)
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
        router.route("/processes/").handler(processQueryService::getProcesses)
        router.route("/engines/").handler(engineHealthCheckService::getHealthyEngineIds)
        router.route("/engines/:deploymentId").handler { engineService.undeployEngine(it, vertx) }
        val serverStart = Promise.promise<Void>()
        server
            .requestHandler(router)
            .listen(8080)
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
        engineService.startProcess(workflowName, it.bodyAsString ?: "", eventBus)
            .setHandler { it1 ->
                if (it1.failed()) {
                    it.fail(it1.cause())
                } else {
                    val response = it.response()
                    response.putHeader("content-type", "text/plain")
                    response.end("Execute workflow! ProcessId: ${it1.result()}")
                }
            }
    }
}