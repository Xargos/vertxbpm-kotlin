package process.infrastructure

import io.vertx.core.Future
import io.vertx.core.Promise
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
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

    init {
        ignite.createCache<ProcessId, FlowContext<Any>?>(processesCacheName)
        ignite.createCache<EngineId, Set<ProcessId>>(enginesCacheName)
        ignite.createCache<NodeId, Set<EngineId>>(nodesCacheName)
        ignite.queue<FlowContext<Any>>(
            waitProcessesQueueName,
            0,
            CollectionConfiguration()
        )
    }

    override fun <T> saveProcess(flowContext: FlowContext<T>, processId: ProcessId): Future<Void> {
        val promise = Promise.promise<Void>()
        GlobalScope.launch {
            val processesCache = ignite.cache<ProcessId, FlowContext<T>?>(processesCacheName)
            processesCache.putAsync(processId, flowContext)
                .listen { promise.complete() }
        }
        return promise.future()
    }

    override fun <T> retrieveProcess(processId: ProcessId): Future<FlowContext<T>?> {
        val promise = Promise.promise<FlowContext<T>?>()
        GlobalScope.launch {
            val cache = ignite.cache<ProcessId, FlowContext<T>?>(processesCacheName)
            cache.getAsync(processId)
                .listen { promise.complete(it.get()) }
        }
        return promise.future()
    }

    override fun retrieveAllProcesses(): List<FlowContext<Any>> {
        try {
            val cache = ignite.cache<String, FlowContext<Any>>(processesCacheName)
            val allValues: IgniteBiPredicate<ProcessId, FlowContext<Any>>? = null
            return cache.query(ScanQuery(allValues)) { it.value }.all
        } catch (ex: Exception) {
            ex.printStackTrace()
            throw RuntimeException(ex)
        }
    }

    override fun getActiveProcesses(engineIds: Set<EngineId>): Future<List<FlowContext<Any>>> {
        val cache = ignite.cache<EngineId, Set<ProcessId>>(enginesCacheName)
        val processIds = cache.getAll(engineIds).values.flatten().toSet()
        val processesCache = ignite.cache<ProcessId, FlowContext<Any>?>(processesCacheName)
        val flowContexts =
            processesCache.getAll(processIds).values
                .filterNotNull()
                .filter { !it.ended }
                .toList()
        return Future.succeededFuture<List<FlowContext<Any>>>(flowContexts)
    }

    override fun assignProcessToEngine(engineId: EngineId, processId: ProcessId): Future<Void> {
        val promise = Promise.promise<Void>()

        GlobalScope.launch {
            val engineCache = ignite.cache<EngineId, Set<ProcessId>>(enginesCacheName)
            engineCache.putIfAbsentAsync(engineId, setOf())
                .listen {
                    engineCache.getAsync(engineId)
                        .listen {
                            engineCache.putAsync(engineId, it.get().plus(processId))
                                .listen { promise.complete() }
                        }
                }
        }

        return promise.future()
    }

    override fun removeProcessFromEngine(engineId: EngineId, processId: ProcessId): Future<Void> {
        val promise = Promise.promise<Void>()
        GlobalScope.launch {
            val engineCache = ignite.cache<EngineId, Set<ProcessId>>(enginesCacheName)
            engineCache.containsKeyAsync(engineId)
                .listen { exists ->
                    if (exists.get()) {
                        engineCache.getAsync(engineId)
                            .listen {
                                engineCache.putAsync(engineId, it.get().minus(processId))
                                    .listen { promise.complete() }
                            }
                    } else {
                        promise.complete()
                    }
                }
        }
        return promise.future()
    }

    override fun assignEngineToNode(nodeId: NodeId, engineId: EngineId): Future<Void> {
        val promise = Promise.promise<Void>()
        GlobalScope.launch {
            val nodesCache = ignite.cache<NodeId, Set<EngineId>>(nodesCacheName)
            nodesCache.putIfAbsent(nodeId, setOf())
            val engineIds = nodesCache.get(nodeId)
            nodesCache.putAsync(nodeId, engineIds.plus(engineId))
                .listen { promise.complete() }
        }

        return promise.future()
    }

    override fun removeDeadEnginesFromCache(
        nodeId: NodeId,
        deadEngineIds: MutableSet<EngineId>
    ): Future<Void> {
        val promise = Promise.promise<Void>()
        GlobalScope.launch {
            val nodesCache = ignite.cache<NodeId, Set<EngineId>>(nodesCacheName)
            if (nodesCache.containsKey(nodeId)) {
                val engineIds = nodesCache.get(nodeId)
                nodesCache.putAsync(nodeId, engineIds.minus(deadEngineIds))
                    .listen { promise.complete() }
            } else {
                promise.complete()
            }
        }

        return promise.future()
    }

    override fun moveDeadNodeProcessesToWaitQueueAndCleanup(nodeId: NodeId): Future<Void> {
        val promise = Promise.promise<Void>()
        GlobalScope.launch {
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
                        promise.complete()
                    } else {
                        val engineIds = nodesCache.get(nodeId)

                        val enginesCache = ignite.cache<EngineId, Set<ProcessId>>(enginesCacheName)
                        println(engineIds)
                        val processIds = enginesCache.getAll(engineIds).values.flatten().toSet()

                        val processesCache = ignite.cache<ProcessId, FlowContext<Any>?>(processesCacheName)
                        val flowContexts =
                            processesCache.getAll(processIds).values
                                .filterNotNull()
                                .filter { it.currentStep !is Step.End<*> }
                                .toList()
                        waitProcessQueue.addAll(flowContexts)
                        enginesCache.removeAll(engineIds)
                        nodesCache.remove(nodeId)
                        tx.commit()
                        promise.complete()
                    }
                } catch (e: Exception) {
                    tx.rollback()
                    e.printStackTrace()
                    promise.fail(e)
                }
            }
        }
        return promise.future()
    }

    override fun getAndExecuteWaitingProcess(exec: (fc: FlowContext<Any>) -> Future<Void>): Future<Void> {
        val promise = Promise.promise<Void>()
        GlobalScope.launch {
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
                        exec.invoke(flowContext)
                            .onFailure {
                                tx.rollback()
                                promise.fail(it)
                            }
                            .onSuccess {
                                tx.commit()
                                promise.complete()
                            }
                    } else {
                        tx.commit()
                        promise.complete()
                    }
                } catch (e: Exception) {
                    tx.rollback()
                    e.printStackTrace()
                    promise.fail(e)
                }
            }
        }
        return promise.future()
    }
}