package process.engine

import io.vertx.core.Future
import io.vertx.core.Vertx
import process.control.NodeId

interface Repository {
    fun init(vertx: Vertx): Future<Void>
    fun <T> saveProcess(flowContext: FlowContext<T>): Future<Void>
    fun <T> getOrCreateProcess(flowContext: FlowContext<T>): Future<FlowContext<T>>
    fun retrieveAllProcesses(): List<FlowContext<Any>>
    fun getActiveProcesses(engineIds: Set<EngineId>): Future<List<FlowContext<Any>>>
    fun startNewProcess(processId: ProcessId): Future<Void>
    fun removeProcessFromEngine(engineId: EngineId, processId: ProcessId): Future<Void>
    fun moveDeadNodeProcessesToWaitQueueAndCleanup(nodeId: NodeId): Future<Void>
    fun assignEngineToNode(nodeId: NodeId, engineId: EngineId): Future<Void>
    fun removeDeadEnginesFromCache(
        nodeId: NodeId,
        deadEngineIds: MutableSet<EngineId>
    ): Future<Void>

    fun getAndExecuteWaitingProcess(exec: (fc: FlowContext<Any>) -> Future<Void>): Future<Void>
}