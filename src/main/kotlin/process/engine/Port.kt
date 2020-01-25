package process.engine

import io.vertx.core.Future

interface Repository {
    fun <T> saveProcess(flowContext: FlowContext<T>, processId: ProcessId): Future<Void>
    fun <T> retrieveProcess(processId: ProcessId): Future<FlowContext<T>?>
    fun retrieveAllProcesses(): List<FlowContext<Any>>
    fun getActiveProcesses(engineIds: Set<EngineId>): Future<List<FlowContext<Any>>>
    fun assignProcessToEngine(engineId: EngineId, processId: ProcessId): Future<Void>
    fun removeProcessFromEngine(engineId: EngineId, processId: ProcessId): Future<Void>
}