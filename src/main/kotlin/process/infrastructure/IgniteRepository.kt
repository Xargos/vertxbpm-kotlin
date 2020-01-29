package process.infrastructure

import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.ignite.Ignite
import org.apache.ignite.configuration.CollectionConfiguration
import process.control.NodeId
import process.engine.*
import javax.cache.Cache


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

    private val newProcesses: MutableList<Pair<ProcessId, Promise<Void>>> = mutableListOf()
    private val newContexts: MutableList<Pair<FlowContext<Any>, Promise<FlowContext<Any>>>> = mutableListOf()
    private val pendingProcessesToSave: MutableList<Pair<FlowContext<Any>, Promise<Void>>> = mutableListOf()

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
        val newContextLocal = (0 until newContexts.size).map { newContexts.removeAt(0) }
        vertx.executeBlocking<Void>({ p ->
            //            println("getting or creating new processes: ${newContextLocal.size}")

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
        val assignProcessToEngineLocal = (0 until newProcesses.size).map { newProcesses.removeAt(0) }
        vertx.executeBlocking<Void>({ p ->
            enginesCache.getAsync(engineId)
                .listen {
                    enginesCache.putAsync(engineId, it.get().plus(assignProcessToEngineLocal.keys))
                        .listen {
                            //                            println("Assigning new processes: ${assignProcessToEngineLocal.size}")
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
        val pendingProcessesToSaveLocal =
            (0 until pendingProcessesToSave.size).map { pendingProcessesToSave.removeAt(0) }
        vertx.executeBlocking<Void>({ p ->

            processesCache.putAllAsync(pendingProcessesToSaveLocal.keys.map { it.processId to it }.toMap())
                .listen {
                    //                    println("Stored ${pendingProcessesToSaveLocal.size} processes")
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

        val itr: Iterator<Cache.Entry<ProcessId, FlowContext<Any>>> = processesCache.iterator()

        val fc: MutableList<FlowContext<Any>> = mutableListOf()
        while (itr.hasNext()) {
            fc.add(itr.next().value)
        }
        return fc
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
        newProcesses.add(Pair(processId, promise))
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