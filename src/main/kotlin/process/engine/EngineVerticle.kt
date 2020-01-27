package process.engine

import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.eventbus.Message
import java.io.Serializable

data class EngineId(val value: String) : Serializable

class EngineVerticle(
    private val engineId: EngineId,
    private val workflowEngine: WorkflowEngine,
    private val workflowStore: WorkflowStore,
    private val startProcessTopic: String
) : AbstractVerticle() {
    override fun start(startPromise: Promise<Void>) {

        val eventBus = vertx.eventBus()
        eventBus.consumer<String>(startProcessTopic) { startProcess(it) }
        eventBus.consumer<String>("{$engineId}_healthcheck") { healthCheck(it) }

        startPromise.complete()
    }

    private fun healthCheck(it: Message<String>) {
        it.reply("")
    }

    private fun startProcess(message: Message<String>) {
        val workflowName = message.headers()["workflowName"]
        val processId = message.headers()["processId"]
        val workflow =
            workflowStore.workflows[workflowName] ?: throw RuntimeException("Unknown workflow: $workflowName")
        val data = workflow.decodeData(message.body())

        workflowEngine.start(workflow, vertx, inputData = data, processId = ProcessId(processId))
        message.reply(processId)
    }
}