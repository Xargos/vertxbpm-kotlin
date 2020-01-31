package process

import de.huxhorn.sulky.ulid.ULID
import io.vertx.core.AsyncResult
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.spi.cluster.ClusterManager
import io.vertx.spi.cluster.ignite.IgniteClusterManager
import org.apache.ignite.Ignition
import process.engine.*
import process.infrastructure.IgniteRepository
import process.verticles.EventBusConfig
import process.verticles.EventBusVerticle
import kotlin.system.exitProcess


fun main() {

    val simpleWorkflow = SimpleWorkflow()
    val workflows = mapOf(simpleWorkflow.name() to (simpleWorkflow as Workflow<Any>))

    val clusterManager: ClusterManager = IgniteClusterManager()

    val options = VertxOptions().setClusterManager(clusterManager)
    Vertx.clusteredVertx(options) { res: AsyncResult<Vertx?> ->
        if (res.succeeded()) {
            val serverVerticle = buildControlVerticle(workflows)
            res.result()?.deployVerticle(serverVerticle) {
                if (it.failed()) {
                    println("Startup failed")
                    it.cause().printStackTrace()
                    exitProcess(-1)
                } else {
                    println("Startup finished successfully")
                }
            }
        } else { // failed!
            throw res.cause()
        }
    }
}

private fun buildControlVerticle(workflows: Map<String, Workflow<Any>>): EventBusVerticle {
    val ulid = ULID()
    Ignition.start()
    val igniteRepository = IgniteRepository(
        waitProcessesQueueName = "waitProcessesQueue",
        processesCacheName = "processes",
        nodesCacheName = "nodes",
        ignite = Ignition.ignite()
    )
    val processQueryService = ProcessQueryService(igniteRepository)
    val nodeSynchronizationService = NodeSynchronizationService(igniteRepository)
    val engineService = EngineService(
        workflowEngine = WorkflowEngine(igniteRepository),
        nodeSynchronizationService = nodeSynchronizationService,
        workflowStore = WorkflowStore(workflows),
        ulid = ulid,
        repository = igniteRepository
    )
    val eventBusConfig = EventBusConfig("start_process", "statistics")
    return EventBusVerticle(
        engineService = engineService,
        processQueryService = processQueryService,
        config = eventBusConfig
    )
}