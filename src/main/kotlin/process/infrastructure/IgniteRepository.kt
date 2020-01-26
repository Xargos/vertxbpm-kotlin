package process.infrastructure

import io.vertx.core.Future
import io.vertx.core.Promise
import org.apache.ignite.Ignite
import org.apache.ignite.cache.query.ScanQuery
import org.apache.ignite.configuration.CollectionConfiguration
import org.apache.ignite.lang.IgniteBiPredicate
import process.control.NodeId
import process.engine.*


// TODO: Optimize stored data structure. Maybe additional indexes for restarting jobs.
//  Make all async.
//  When cleaning up dead nodes remove also all the engines and processes mappings.!
class IgniteRepository(
    private val processesCacheName: String,
    private val enginesCacheName: String,
    private val nodesCacheName: String,
    private val waitProcessesQueueName: String,
    private val ignite: Ignite
) : Repository {

    override fun <T> saveProcess(flowContext: FlowContext<T>, processId: ProcessId): Future<Void> {
        try {
            val promise = Promise.promise<Void>()
            val processesCache = ignite.getOrCreateCache<ProcessId, FlowContext<T>?>(processesCacheName)
            processesCache.putAsync(processId, flowContext)
                .listen { promise.complete() }
            return promise.future()
        } catch (ex: Exception) {
            ex.printStackTrace()
            throw RuntimeException(ex)
        }
    }

    override fun <T> retrieveProcess(processId: ProcessId): Future<FlowContext<T>?> {
        try {
            val promise = Promise.promise<FlowContext<T>?>()
            val cache = ignite.getOrCreateCache<ProcessId, FlowContext<T>?>(processesCacheName)
            cache.getAsync(processId)
                .listen { promise.complete(it.get()) }
            return promise.future()
        } catch (ex: Exception) {
            ex.printStackTrace()
            throw RuntimeException(ex)
        }
    }

    override fun retrieveAllProcesses(): List<FlowContext<Any>> {
        try {
            val cache = ignite.getOrCreateCache<String, FlowContext<Any>>(processesCacheName)
            val allValues: IgniteBiPredicate<ProcessId, FlowContext<Any>>? = null
            return cache.query(ScanQuery(allValues)) { it.value }.all
        } catch (ex: Exception) {
            ex.printStackTrace()
            throw RuntimeException(ex)
        }
    }

    override fun getActiveProcesses(engineIds: Set<EngineId>): Future<List<FlowContext<Any>>> {
        val cache = ignite.getOrCreateCache<EngineId, Set<ProcessId>>(enginesCacheName)
        val processIds = cache.getAll(engineIds).values.flatten().toSet()
        val processesCache = ignite.getOrCreateCache<ProcessId, FlowContext<Any>?>(processesCacheName)
        val flowContexts =
            processesCache.getAll(processIds).values
                .filterNotNull()
                .filter { it.currentStep !is Step.End<*> }
                .toList()
        return Future.succeededFuture<List<FlowContext<Any>>>(flowContexts)
    }

    override fun assignProcessToEngine(engineId: EngineId, processId: ProcessId): Future<Void> {
        val promise = Promise.promise<Void>()
        val engineCache = ignite.getOrCreateCache<EngineId, Set<ProcessId>>(enginesCacheName)
        engineCache.putIfAbsent(engineId, setOf())
        val processIds = engineCache.get(engineId)
        engineCache.putAsync(engineId, processIds.plus(processId))
            .listen { promise.complete() }

        return promise.future()
    }

    override fun removeProcessFromEngine(engineId: EngineId, processId: ProcessId): Future<Void> {
        val promise = Promise.promise<Void>()
        val engineCache = ignite.getOrCreateCache<EngineId, Set<ProcessId>>(enginesCacheName)
        if (engineCache.containsKey(engineId)) {
            val processIds = engineCache.get(engineId)
            engineCache.putAsync(engineId, processIds.minus(processId))
                .listen { promise.complete() }
        } else {
            promise.complete()
        }

        return promise.future()
    }

    override fun assignEngineToNode(nodeId: NodeId, engineId: EngineId): Future<Void> {
        val promise = Promise.promise<Void>()
        val nodesCache = ignite.getOrCreateCache<NodeId, Set<EngineId>>(nodesCacheName)
        nodesCache.putIfAbsent(nodeId, setOf())
        val engineIds = nodesCache.get(nodeId)
        nodesCache.putAsync(nodeId, engineIds.plus(engineId))
            .listen { promise.complete() }

        return promise.future()
    }

    override fun removeDeadEnginesFromCache(
        nodeId: NodeId,
        deadEngineIds: MutableSet<EngineId>
    ): Future<Void> {
        val promise = Promise.promise<Void>()
        val nodesCache = ignite.getOrCreateCache<NodeId, Set<EngineId>>(nodesCacheName)
        if (nodesCache.containsKey(nodeId)) {
            val engineIds = nodesCache.get(nodeId)
            nodesCache.putAsync(nodeId, engineIds.minus(deadEngineIds))
                .listen { promise.complete() }
        } else {
            promise.complete()
        }

        return promise.future()
    }

    override fun moveDeadNodeProcessesToWaitQueueAndCleanup(nodeId: NodeId) {
        val transactions = ignite.transactions()
        val waitProcessQueue = ignite.queue<FlowContext<Any>>(
            waitProcessesQueueName,
            0,
            CollectionConfiguration()
        )
        transactions.txStart().use { tx ->
            try {
                val nodesCache = ignite.cache<NodeId, Set<EngineId>>(nodesCacheName)
                if (!nodesCache.containsKey(nodeId)) {
                    tx.rollback()
                    return
                }
                val engineIds = nodesCache.get(nodeId)

                val enginesCache = ignite.cache<EngineId, Set<ProcessId>>(enginesCacheName)
                println(engineIds)
                val processIds = enginesCache.getAll(engineIds).values.flatten().toSet()

                val processesCache = ignite.getOrCreateCache<ProcessId, FlowContext<Any>?>(processesCacheName)
                val flowContexts =
                    processesCache.getAll(processIds).values
                        .filterNotNull()
                        .filter { it.currentStep !is Step.End<*> }
                        .toList()
                waitProcessQueue.addAll(flowContexts)
                enginesCache.removeAll(engineIds)
                nodesCache.remove(nodeId)
                tx.commit()
            } catch (e: Exception) {
                tx.rollback()
                e.printStackTrace()
            }
        }
    }

    override fun getAndExecuteWaitingProcess(exec: (fc: FlowContext<Any>) -> Future<Void>): Future<Void> {
        val transactions = ignite.transactions()
        val waitProcessQueue = ignite.queue<FlowContext<Any>>(
            waitProcessesQueueName,
            0,
            CollectionConfiguration()
        )
        transactions.txStart().use { tx ->
            try {
                val flowContext = waitProcessQueue.poll()
                if (flowContext != null) {
                    return exec.invoke(flowContext)
                        .compose {
                            tx.commit()
                            Future.succeededFuture<Void>()
                        }
                } else {
                    tx.commit()
                }
            } catch (e: Exception) {
                tx.rollback()
                e.printStackTrace()
            }
        }
        return Future.succeededFuture<Void>()
    }
}