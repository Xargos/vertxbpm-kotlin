package process.engine

import io.vertx.core.Future
import io.vertx.core.Vertx


interface Repository {
    fun init(vertx: Vertx): Future<Void>
    fun <T> saveProcess(flowContext: FlowContext<T>): Future<Void>
    fun <T> getOrCreateProcess(flowContext: FlowContext<T>): Future<FlowContext<T>>
    fun retrieveAllProcesses(): List<FlowContext<Any>>
    fun startNewProcess(processId: ProcessId): Future<Void>
    fun removeProcessFromEngine(processId: ProcessId): Future<Void>
    fun moveDeadNodeProcessesToWaitQueueAndCleanup(nodeId: NodeId): Future<Void>

    fun getAndExecuteWaitingProcess(exec: (fc: FlowContext<Any>) -> Future<Void>): Future<Void>
}