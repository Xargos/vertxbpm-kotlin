package process.engine

import io.vertx.core.Future
import io.vertx.core.Vertx


interface Repository {
    fun init(vertx: Vertx, nodeId: NodeId): Future<Void>
    fun <T> saveProcess(flowContext: FlowContext<T>): Future<Void>
    fun <T> finishProcess(flowContext: FlowContext<T>): Future<Void>
    fun <T> getOrCreateProcess(flowContext: FlowContext<T>): Future<FlowContext<T>>
    fun <T> getProcess(processId: ProcessId): Future<FlowContext<T>?>
    fun retrieveAllProcesses(): Future<Set<FlowContext<Any>>>
    fun startNewProcess(processId: ProcessId): Future<Void>
    fun removeProcessFromEngine(processId: ProcessId): Future<Void>
    fun moveDeadNodeProcessesToWaitQueueAndCleanup(nodeId: NodeId): Future<Void>

    fun getAndExecuteWaitingProcess(exec: (fc: FlowContext<Any>) -> Future<Void>): Future<Void>
}