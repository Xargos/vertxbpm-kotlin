package process.engine

import de.huxhorn.sulky.ulid.ULID
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.impl.VertxImpl
import java.io.Serializable

data class NodeId(val value: String) : Serializable


class EngineService(
    private val engine: Engine,
    private val workflowStore: WorkflowStore,
    private val nodeSynchronizationService: NodeSynchronizationService,
    private val ulid: ULID,
    private val repository: Repository
) {

    fun start(vertx: Vertx) {
        val nodeId = NodeId((vertx as VertxImpl).nodeID)
        repository.init(vertx, nodeId)
        nodeSynchronizationService.subscribeNodeExistence(vertx)
        nodeSynchronizationService.listenToWaitingProcesses(vertx, this, nodeId)
    }

    fun startProcess(
        workflowName: String,
        body: String,
        processId: ProcessId = ProcessId(ulid.nextULID())
    ): Future<ProcessId> {
        val workflow =
            workflowStore.workflows[workflowName] ?: throw RuntimeException("Unknown workflow: $workflowName")
        val data = workflow.decodeData(body)
        return engine.start(workflow, data, processId)
            .compose { Future.succeededFuture(processId) }
    }
}