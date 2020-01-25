package process.engine

import io.vertx.core.Future

interface Repository {
    fun <T> saveProcess(flowContext: FlowContext<T>, processId: ProcessId, engineId: EngineId): Future<Void>
    fun <T> retrieveProcess(processId: ProcessId): Future<FlowContext<T>?>
    fun retrieveAllProcesses(): List<FlowContext<Any>>
    fun getActiveProcesses(engineIds: Set<EngineId>): Future<List<FlowContext<Any>>>
}