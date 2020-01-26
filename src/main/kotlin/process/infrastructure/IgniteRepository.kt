package process.infrastructure

import io.vertx.core.Future
import io.vertx.core.Promise
import org.apache.ignite.Ignite
import org.apache.ignite.cache.query.ScanQuery
import org.apache.ignite.lang.IgniteBiPredicate
import process.control.NodeId
import process.engine.*

// TODO: Optimize stored data structure. Maybe additional indexes for restarting jobs.
//  Make all async.
//  When cleaning up dead nodes remove also all the engines and processes mappings.!
class IgniteRepository(
    private val globalCacheName: String,
    private val processesCacheName: String,
    private val enginesCacheName: String,
    private val nodesCacheName: String,
    private val ignite: Ignite
) : Repository {

    private val nodesCacheKey = "NODES"

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

    override fun subscribeNodeExistence(nodeId: NodeId): Future<Void> {
        val promise = Promise.promise<Void>()
        val nodesCache = ignite.getOrCreateCache<String, Set<NodeId>>(globalCacheName)
        nodesCache.putIfAbsent(nodesCacheKey, setOf())
        val nodeIds = nodesCache.get(nodesCacheKey)
        nodesCache.putAsync(nodesCacheKey, nodeIds.plus(nodeId))
            .listen { promise.complete() }

        return promise.future()
    }

    override fun removeNodesExistence(deadNodes: Set<NodeId>): Future<Void> {
        val promise = Promise.promise<Void>()
        val nodesCache = ignite.getOrCreateCache<String, Set<NodeId>>(globalCacheName)
        if (nodesCache.containsKey(nodesCacheKey)) {
            val nodeIds = nodesCache.get(nodesCacheKey)
            nodesCache.putAsync(nodesCacheKey, nodeIds.minus(deadNodes))
                .listen { promise.complete() }
        } else {
            promise.complete()
        }

        return promise.future()
    }

    override fun getSubscribedNodes(): Future<Set<NodeId>> {
        val promise = Promise.promise<Set<NodeId>>()
        val nodesCache = ignite.getOrCreateCache<String, Set<NodeId>>(globalCacheName)
        nodesCache.getAsync(nodesCacheKey)
            .listen { promise.complete(it.get() ?: setOf()) }

        return promise.future()
    }

    override fun getProcessesOfDeadNodes(deadNodes: Set<NodeId>): Future<List<FlowContext<Any>>> {
        val promise = Promise.promise<List<FlowContext<Any>>>()
        val nodesCache = ignite.getOrCreateCache<NodeId, Set<EngineId>>(nodesCacheName)
        nodesCache.getAllAsync(deadNodes)
            .listen { nodes ->
                getActiveProcesses(nodes.get().map { it.value }.filterNotNull().flatten().toSet())
                    .setHandler(promise::handle)
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
}