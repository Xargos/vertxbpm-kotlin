package process.verticles

import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.eventbus.Message
import io.vertx.core.json.Json
import process.engine.EngineService
import process.engine.ProcessQueryService
import java.io.Serializable

data class EventBusConfig(
    val startProcessTopicAddress: String,
    val statisticsAddress: String
) : Serializable

class EventBusVerticle(
    private val engineService: EngineService,
    private val config: EventBusConfig,
    private val processQueryService: ProcessQueryService
) : AbstractVerticle() {
    override fun start(startPromise: Promise<Void>) {
        println(this.vertx.deploymentIDs())

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