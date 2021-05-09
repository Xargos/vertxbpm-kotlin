package process.engine

import io.vertx.core.Future

data class Statistics(val processCount: Int, val activeProcessCount: Int, val finishedProcessesCount: Int)

class ProcessQueryService(
    private val workflowEngineRepository: WorkflowEngineRepository
) {

    fun <T> getProcess(processId: ProcessId): Future<FlowContext<T>?> {
        return workflowEngineRepository.getProcess(processId)
    }

    fun getProcesses(): Future<Set<FlowContext<Any>>> {
        return workflowEngineRepository.retrieveAllProcesses()
    }

    fun getStatistics(): Future<Statistics> {
        return workflowEngineRepository.retrieveAllProcesses()
            .compose { allProcesses ->
                Future.succeededFuture(
                    Statistics(
                        processCount = allProcesses.count(),
                        activeProcessCount = allProcesses.filter { !it.ended }.count(),
                        finishedProcessesCount = allProcesses.filter { it.ended }.count()
                    )
                )
            }
    }
}