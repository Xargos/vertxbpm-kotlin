package process.verticles

import process.engine.InstantWorkflow
import process.engine.LongWorkflow
import de.huxhorn.sulky.ulid.ULID
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.Json
import io.vertx.core.spi.cluster.ClusterManager
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.spi.cluster.ignite.IgniteClusterManager
import org.apache.ignite.Ignite
import process.engine.*
import process.infrastructure.IgniteWorkflowEngineRepository
import java.io.Serializable

data class HttpConfig(val port: Int) : Serializable

fun buildHttpVerticle(
    clusterManager: IgniteClusterManager,
    longWorkflows: Map<String, () -> LongWorkflow<Any>> = mapOf(),
    instantWorkflows: Map<String, () -> InstantWorkflow<Any, Any>> = mapOf()
): HttpVerticle {
    val ulid = ULID()
    val igniteRepository = IgniteWorkflowEngineRepository(
        waitProcessesQueueName = "waitProcessesQueue",
        finishedProcessesCacheName = "finishedProcesses",
        processesCacheName = "processes",
        nodesCacheName = "nodes",
        ignite = clusterManager.igniteInstance
    )
    val processQueryService = ProcessQueryService(igniteRepository)
    val nodeSynchronizationService = NodeSynchronizationService(igniteRepository)
    val engineService = EngineService(
        engine = Engine(igniteRepository),
        nodeSynchronizationService = nodeSynchronizationService,
        workflowStore = WorkflowStore(
            instantWorkflows = instantWorkflows,
            longWorkflows = longWorkflows
        ),
        ulid = ulid,
        workflowEngineRepository = igniteRepository,
        clusterManager = clusterManager
    )
    val httpConfig = HttpConfig(8080)
    return HttpVerticle(
        engineService = engineService,
        processQueryService = processQueryService,
        config = httpConfig
    )
}

class HttpVerticle(
    private val engineService: EngineService,
    private val processQueryService: ProcessQueryService,
    private val config: HttpConfig
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

        router.route(HttpMethod.POST, "/workflow/:workflowName").handler { startProcess(it) }

        router.route("/processes/:processId").handler { getProcess(it) }
        router.route("/processes/").handler { getProcesses(it) }
        router.route("/statistics/").handler { getStatistics(it) }
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

    private fun getProcess(routingContext: RoutingContext) {
        val processId = ProcessId(routingContext.pathParam("processId"))
        processQueryService.getProcess<Any>(processId)
            .onSuccess { sendResponse(routingContext, it) }
            .onFailure { routingContext.fail(it) }
    }

    private fun getProcesses(routingContext: RoutingContext) {
        processQueryService.getProcesses()
            .onSuccess { sendResponse(routingContext, it) }
            .onFailure { routingContext.fail(it) }
    }

    private fun getStatistics(routingContext: RoutingContext) {
        processQueryService.getStatistics()
            .onSuccess { sendResponse(routingContext, it) }
            .onFailure { routingContext.fail(it) }
    }

    private fun startProcess(routingContext: RoutingContext) {
        val workflowName = routingContext.pathParam("workflowName")
        engineService.startProcess(workflowName, routingContext.bodyAsString ?: "")
            .onSuccess { sendResponse(routingContext, it) }
            .onFailure { routingContext.fail(it) }
    }

    private fun <R> sendResponse(routingContext: RoutingContext, it: R) {
        val response = routingContext.response()
        response.putHeader("content-type", "application/json")
        response.end(Json.encode(it))
    }
}