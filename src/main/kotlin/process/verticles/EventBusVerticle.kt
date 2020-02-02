package process.verticles

import de.huxhorn.sulky.ulid.ULID
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.eventbus.Message
import io.vertx.core.json.Json
import io.vertx.core.spi.cluster.ClusterManager
import org.apache.ignite.Ignite
import process.engine.*
import process.infrastructure.IgniteRepository
import java.io.Serializable

data class EventBusConfig(
    val startProcessTopicAddress: String,
    val statisticsAddress: String
) : Serializable

fun buildEventBusVerticle(
    workflows: Map<String, Workflow<Any>>,
    ignite: Ignite,
    clusterManager: ClusterManager
): EventBusVerticle {
    val ulid = ULID()
    val igniteRepository = IgniteRepository(
        waitProcessesQueueName = "waitProcessesQueue",
        finishedProcessesCacheName = "finishedProcesses",
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
        repository = igniteRepository,
        clusterManager = clusterManager
    )
    val eventBusConfig = EventBusConfig("start_process", "statistics")
    return EventBusVerticle(
        engineService = engineService,
        processQueryService = processQueryService,
        config = eventBusConfig
    )
}

class EventBusVerticle(
    private val engineService: EngineService,
    private val config: EventBusConfig,
    private val processQueryService: ProcessQueryService
) : AbstractVerticle() {
    override fun start(startPromise: Promise<Void>) {
        val eventBus = vertx.eventBus()
        eventBus.consumer<String>(config.startProcessTopicAddress) { startProcess(it) }
        eventBus.consumer<String>(config.statisticsAddress) { getStatistics(it) }

        engineService.start(vertx)

        startPromise.complete()
    }

    private fun startProcess(message: Message<String>) {
        val workflowName = message.headers()["workflowName"]
        engineService.startProcess(workflowName, message.body())
            .onSuccess { message.reply(it.value) }
            .onFailure { message.fail(-1, it.cause?.message) }
    }

    private fun getStatistics(message: Message<String>) {
        processQueryService.getStatistics()
            .onSuccess { message.reply(Json.encode(it)) }
            .onFailure { message.fail(-1, it.cause?.message) }
    }
}