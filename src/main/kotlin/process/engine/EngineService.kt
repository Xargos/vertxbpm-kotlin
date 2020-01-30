package process.engine

import de.huxhorn.sulky.ulid.ULID
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.EventBus
import io.vertx.core.impl.VertxImpl
import java.io.Serializable

data class NodeId(val value: String) : Serializable

class EngineService(
    private val workflowEngine: WorkflowEngine,
    private val workflowStore: WorkflowStore,
    private val nodeSynchronizationService: NodeSynchronizationService,
    private val ulid: ULID,
    private val vertx: Vertx
) {

    fun start() {
        val nodeId = NodeId((vertx as VertxImpl).nodeID)
        nodeSynchronizationService.subscribeNodeExistence(vertx)
        nodeSynchronizationService.listenToWaitingProcesses(vertx, this, nodeId)
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
}