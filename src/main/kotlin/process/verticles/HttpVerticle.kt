package process.verticles

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import process.engine.EngineService
import process.engine.ProcessQueryService
import java.io.Serializable

data class HttpConfig(val port: Int) : Serializable

class HttpVerticle(
    private val engineService: EngineService,
    private val processQueryService: ProcessQueryService,
    private val httpConfig: HttpConfig
) : AbstractVerticle() {

    override fun start(startPromise: Promise<Void>) {

        engineService.start(vertx)

        startHttpServer()
            .onSuccess { startPromise.complete() }
            .onFailure { startPromise.fail(it) }
    }

    private fun startHttpServer(): Future<Void> {
        val server = vertx.createHttpServer()

        val router = Router.router(vertx)

        router.route("/workflow/:workflowName").handler { startProcess(it) }

        router.route("/processes/:processId").handler(processQueryService::getProcess)
        router.route("/statistics/count").handler { routingContext ->
            processQueryService.getStatistics()
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
        val serverStart = Promise.promise<Void>()
        server
            .requestHandler(router)
            .listen(httpConfig.port)
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

    private fun startProcess(it: RoutingContext) {
        val workflowName = it.pathParam("workflowName")
        engineService.startProcess(workflowName, it.bodyAsString ?: "")
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