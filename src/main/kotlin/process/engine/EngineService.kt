package process.engine

import de.huxhorn.sulky.ulid.ULID
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.spi.cluster.ClusterManager
import java.io.Serializable

data class NodeId(val value: String) : Serializable


class EngineService(
    private val engine: Engine,
    private val workflowStore: WorkflowStore,
    private val nodeSynchronizationService: NodeSynchronizationService,
    private val ulid: ULID,
    private val workflowEngineRepository: WorkflowEngineRepository,
    private val clusterManager: ClusterManager
) {

    fun start(vertx: Vertx) {
        val nodeId = NodeId(clusterManager.nodeId)
        workflowEngineRepository.init(vertx, nodeId)
        nodeSynchronizationService.subscribeNodeExistence(clusterManager)
        nodeSynchronizationService.listenToWaitingProcesses(vertx, this, nodeId)
    }

    fun startInstantProcess(
        workflowName: String,
        body: String,
        processId: ProcessId = ProcessId(ulid.nextULID())
    ): Future<Any> {
        val workflowFactory =
            (workflowStore.instantWorkflows[workflowName] ?: throw RuntimeException("Unknown workflow: $workflowName"))
        val workflow = workflowFactory()
        val data = workflow.decodeData(body)
        return engine.startInstant(workflow, data, processId)
            .compose { Future.succeededFuture(it) }
    }

    fun startProcess(
        workflowName: String,
        body: String,
        processId: ProcessId = ProcessId(ulid.nextULID())
    ): Future<ProcessId> {
        val workflowFactory =
            (workflowStore.longWorkflows[workflowName] ?: throw RuntimeException("Unknown workflow: $workflowName"))
        val workflow = workflowFactory()
        val data = workflow.decodeData(body)
        return engine.start(workflow, data, processId)
            .compose { Future.succeededFuture(processId) }
    }
}