package process.infrastructure

import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
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
    private val engineId: EngineId,
    processesCacheName: String,
    enginesCacheName: String,
    nodesCacheName: String,
    waitProcessesQueueName: String,
    ignite: Ignite
) : Repository {

    private val transactions = ignite.transactions()
    private val processesCache = ignite.createCache<ProcessId, FlowContext<Any>>(processesCacheName)
    private val enginesCache = ignite.createCache<EngineId, Set<ProcessId>>(enginesCacheName)
    private val nodesCache = ignite.createCache<NodeId, Set<EngineId>>(nodesCacheName)
    private val waitProcessesQueue = ignite.queue<FlowContext<Any>>(
        waitProcessesQueueName,
        0,
        CollectionConfiguration()
    )

    private val newProcesses: MutableMap<ProcessId, Promise<Void>> = mutableMapOf()
    private val newContexts: MutableMap<FlowContext<Any>, Promise<FlowContext<Any>>> = mutableMapOf()
    private val pendingProcessesToSave: MutableMap<FlowContext<Any>, Promise<Void>> = mutableMapOf()

    override fun init(vertx: Vertx): Future<Void> {
        enginesCache.putIfAbsentAsync(engineId, setOf())
        runBatchUpload(vertx)
        return Future.succeededFuture()
    }

    private fun runBatchUpload(vertx: Vertx) {
        vertx.setTimer(10) {
            if (newProcesses.isNotEmpty() || newContexts.isNotEmpty() || pendingProcessesToSave.isNotEmpty()) {
                CompositeFuture.join(
                    assignNewProcessesToEngine(vertx),
                    runGetOrCreateProcess(vertx),
                    runStoreSteps(vertx)
                )
                    .onComplete { runBatchUpload(vertx) }
            } else {
                runBatchUpload(vertx)
            }
        }
    }

    private fun runGetOrCreateProcess(vertx: Vertx): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking<Void>({ p ->
            val newContextLocal = newContexts.toMap()
            newContexts.clear()
            println("getting or creating new processes: ${newContextLocal.size}")

            processesCache.getAllAsync(newContextLocal.keys.map { it.processId }.toSet())
                .listen { storedProcesses ->
                    val sp = storedProcesses.get()

                    val unsavedContexts = newContextLocal
                        .filter { !sp.containsKey(it.key.processId) }

                    processesCache.putAllAsync(unsavedContexts.map { it.key.processId to it.key }.toMap())
                        .listen {
                            newContextLocal
                                .filter { sp.containsKey(it.key.processId) }
                                .forEach { it.value.complete(sp[it.key.processId]) }
                            unsavedContexts
                                .forEach { it.value.complete(it.key) }
                            p.complete()
                        }
                }
        }) {
            promise.complete()
        }

        return promise.future()
    }

    private fun assignNewProcessesToEngine(vertx: Vertx): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking<Void>({ p ->
            val assignProcessToEngineLocal = newProcesses.toMap()
            newProcesses.clear()
            enginesCache.getAsync(engineId)
                .listen {
                    enginesCache.putAsync(engineId, it.get().plus(assignProcessToEngineLocal.keys))
                        .listen {
                            println("Assigning new processes: ${assignProcessToEngineLocal.size}")
                            assignProcessToEngineLocal.values.forEach { p -> p.complete() }
//                        println("All uploaded")
                            p.complete()
                        }
                }
        }) {
            promise.complete()
        }

        return promise.future()
    }


    private fun runStoreSteps(vertx: Vertx): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking<Void>({ p ->
            val pendingProcessesToSaveLocal = pendingProcessesToSave.toMap()
            pendingProcessesToSave.clear()

            processesCache.putAllAsync(pendingProcessesToSaveLocal.keys.map { it.processId to it }.toMap())
                .listen {
                    println("Stored ${pendingProcessesToSaveLocal.size} processes")
                    pendingProcessesToSaveLocal.values.forEach { it.complete() }
                    p.complete()
                }
        }) {
            promise.complete()
        }

        return promise.future()
    }

    override fun <T> saveProcess(flowContext: FlowContext<T>): Future<Void> {
        val promise = Promise.promise<Void>()
        pendingProcessesToSave[FlowContext(
            flowContext.workflowName,
            flowContext.processId,
            StepContext(flowContext.currentStep.stepName, flowContext.currentStep.data as Any),
            flowContext.history.map { StepContext(it.stepName, it.data as Any) },
            flowContext.ended
        )] = promise
        return promise.future()
    }

    override fun <T> getOrCreateProcess(flowContext: FlowContext<T>): Future<FlowContext<T>> {
        val promiseAny = Promise.promise<FlowContext<Any>>()
        newContexts[FlowContext(
            flowContext.workflowName,
            flowContext.processId,
            StepContext(flowContext.currentStep.stepName, flowContext.currentStep.data as Any),
            flowContext.history.map { StepContext(it.stepName, it.data as Any) },
            flowContext.ended
        )] = promiseAny
        return promiseAny.future()
            .compose {
                Future.succeededFuture(
                    FlowContext(
                        it.workflowName,
                        it.processId,
                        StepContext(it.currentStep.stepName, it.currentStep.data as T),
                        it.history.map { it1 -> StepContext(it1.stepName, it1.data as T) },
                        it.ended
                    )
                )
            }
    }

    override fun retrieveAllProcesses(): List<FlowContext<Any>> {
        try {
            val allValues: IgniteBiPredicate<ProcessId, FlowContext<Any>>? = null
            return processesCache.query(ScanQuery(allValues)) { it.value }.all
        } catch (ex: Exception) {
            ex.printStackTrace()
            throw RuntimeException(ex)
        }
    }

    override fun getActiveProcesses(engineIds: Set<EngineId>): Future<List<FlowContext<Any>>> {
        val processIds = enginesCache.getAll(engineIds).values.flatten().toSet()
        val flowContexts =
            processesCache.getAll(processIds).values
                .filterNotNull()
                .filter { !it.ended }
                .toList()
        return Future.succeededFuture<List<FlowContext<Any>>>(flowContexts)
    }

    override fun startNewProcess(processId: ProcessId): Future<Void> {
        val promise = Promise.promise<Void>()
        newProcesses[processId] = promise
        return promise.future()
    }

    override fun removeProcessFromEngine(engineId: EngineId, processId: ProcessId): Future<Void> {
        val promise = Promise.promise<Void>()
//        GlobalScope.launch {
//            val engineCache = ignite.cache<EngineId, Set<ProcessId>>(enginesCacheName)
//            engineCache.containsKeyAsync(engineId)
//                .listen { exists ->
//                    if (exists.get()) {
//                        engineCache.getAsync(engineId)
//                            .listen {
//                                engineCache.putAsync(engineId, it.get().minus(processId))
//                                    .listen { promise.complete() }
//                            }
//                    } else {
//                        promise.complete()
//                    }
//                }
//        }
        promise.complete()
        return promise.future()
    }

    override fun assignEngineToNode(nodeId: NodeId, engineId: EngineId): Future<Void> {
        val promise = Promise.promise<Void>()
        GlobalScope.launch {
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
            transactions.txStart().use { tx ->
                try {
                    if (!nodesCache.containsKey(nodeId)) {
                        tx.rollback()
                        promise.complete()
                    } else {
                        val engineIds = nodesCache.get(nodeId)

                        println(engineIds)
                        val processIds = enginesCache.getAll(engineIds).values.flatten().toSet()

                        val flowContexts =
                            processesCache.getAll(processIds).values
                                .filterNotNull()
                                .filter { it.currentStep !is Step.End<*> }
                                .toList()
                        waitProcessesQueue.addAll(flowContexts)
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
            transactions.txStart().use { tx ->
                try {
                    val flowContext = waitProcessesQueue.poll()
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