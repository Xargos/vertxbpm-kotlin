package process

import de.huxhorn.sulky.ulid.ULID
import io.vertx.core.AsyncResult
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.spi.cluster.ClusterManager
import io.vertx.spi.cluster.ignite.IgniteClusterManager
import org.apache.ignite.Ignite
import org.apache.ignite.Ignition
import process.engine.*
import process.infrastructure.IgniteRepository
import process.verticles.EventBusConfig
import process.verticles.EventBusVerticle
import kotlin.system.exitProcess


fun main() {

    val simpleWorkflow = simpleWorkflow()
    val workflows = mapOf(simpleWorkflow.name to (simpleWorkflow as Workflow<Any>))

    Ignition.start()
    val ignite = Ignition.ignite()
    val clusterManager: ClusterManager = IgniteClusterManager(ignite)

    val options = VertxOptions().setClusterManager(clusterManager)
    Vertx.clusteredVertx(options) { res: AsyncResult<Vertx?> ->
        if (res.succeeded()) {
            (1..2).forEach {
                res.result()?.deployVerticle(buildEventBusVerticle(workflows, ignite)) {
                    if (it.failed()) {
                        println("Startup failed")
                        it.cause().printStackTrace()
                        exitProcess(-1)
                    } else {
                        println("Startup finished successfully")
                    }
                }
            }
        } else { // failed!
            throw res.cause()
        }
    }
}

private fun buildEventBusVerticle(
    workflows: Map<String, Workflow<Any>>,
    ignite: Ignite
): EventBusVerticle {
    val ulid = ULID()
    val igniteRepository = IgniteRepository(
        waitProcessesQueueName = "waitProcessesQueue",
        processesCacheName = "processes",
        nodesCacheName = "nodes",
        ignite = ignite
    )
    val processQueryService = ProcessQueryService(igniteRepository)
    val nodeSynchronizationService = NodeSynchronizationService(igniteRepository)
    val engineService = EngineService(
        engine = Engine(igniteRepository),
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