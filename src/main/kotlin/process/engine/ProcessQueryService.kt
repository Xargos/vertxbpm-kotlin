package process.engine

import io.vertx.core.Future

data class Statistics(val processCount: Int, val activeProcessCount: Int, val finishedProcessesCount: Int)

class ProcessQueryService(
    private val repository: Repository
) {

    fun <T> getProcess(processId: ProcessId): Future<FlowContext<T>?> {
        return repository.getProcess(processId)
    }

    fun getProcesses(): Future<Set<FlowContext<Any>>> {
        return repository.retrieveAllProcesses()
    }

    fun getStatistics(): Future<Statistics> {
        return repository.retrieveAllProcesses()
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