package process.infrastructure

import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.ignite.Ignite
import org.apache.ignite.configuration.CollectionConfiguration
import process.engine.*
import javax.cache.Cache


// TODO: Optimize stored data structure. Maybe additional indexes for restarting jobs.
//  Make all async.
//  When cleaning up dead nodes remove also all the engines and processes mappings.!
class IgniteRepository(
    private val nodeId: NodeId,
    processesCacheName: String,
    nodesCacheName: String,
    waitProcessesQueueName: String,
    ignite: Ignite
) : Repository {

    private val transactions = ignite.transactions()
    private val processesCache = ignite.createCache<ProcessId, FlowContext<Any>>(processesCacheName)
    private val nodesCache = ignite.createCache<NodeId, Set<ProcessId>>(nodesCacheName)
    private val waitProcessesQueue = ignite.queue<FlowContext<Any>>(
        waitProcessesQueueName,
        0,
        CollectionConfiguration()
    )

    private val newProcesses: MutableList<Pair<ProcessId, Promise<Void>>> = mutableListOf()
    private val newContexts: MutableList<Pair<FlowContext<Any>, Promise<FlowContext<Any>>>> = mutableListOf()
    private val pendingProcessesToSave: MutableList<Pair<FlowContext<Any>, Promise<Void>>> = mutableListOf()

    override fun init(vertx: Vertx): Future<Void> {
        nodesCache.putIfAbsentAsync(nodeId, setOf())
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
        vertx.executeBlocking<Void>({ p ->
            val assignProcessToEngineLocal = newProcesses.toMap()
            newProcesses.clear()
            nodesCache.getAsync(nodeId)
                .listen {
                    nodesCache.putAsync(nodeId, it.get().plus(assignProcessToEngineLocal.keys))
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
        vertx.executeBlocking<Void>({ p ->

            val pendingProcessesToSaveLocal = pendingProcessesToSave.toMap()
            pendingProcessesToSave.clear()
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
        pendingProcessesToSave.add(
            Pair(
                FlowContext(
                    flowContext.workflowName,
                    flowContext.processId,
                    StepContext(flowContext.currentStep.stepName, flowContext.currentStep.data as Any),
                    flowContext.history.map { StepContext(it.stepName, it.data as Any) },
                    flowContext.ended
                ), promise
            )
        )
        return promise.future()
    }

    override fun <T> getOrCreateProcess(flowContext: FlowContext<T>): Future<FlowContext<T>> {
        val promiseAny = Promise.promise<FlowContext<Any>>()
        newContexts.add(
            Pair(
                FlowContext(
                    flowContext.workflowName,
                    flowContext.processId,
                    StepContext(flowContext.currentStep.stepName, flowContext.currentStep.data as Any),
                    flowContext.history.map { StepContext(it.stepName, it.data as Any) },
                    flowContext.ended
                ), promiseAny
            )
        )
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

    override fun startNewProcess(processId: ProcessId): Future<Void> {
        val promise = Promise.promise<Void>()
        newProcesses.add(Pair(processId, promise))
        return promise.future()
    }

    override fun removeProcessFromEngine(processId: ProcessId): Future<Void> {
        val promise = Promise.promise<Void>()
        nodesCache.containsKeyAsync(nodeId)
            .listen { exists ->
                if (exists.get()) {
                    nodesCache.getAsync(nodeId)
                        .listen {
                            nodesCache.putAsync(nodeId, it.get().minus(processId))
                                .listen { promise.complete() }
                        }
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
                        val processIds = nodesCache.get(nodeId)

                        println(processIds)

                        val flowContexts =
                            processesCache.getAll(processIds).values
                                .filterNotNull()
                                .filter { it.currentStep !is Step.End<*> }
                                .toList()
                        waitProcessesQueue.addAll(flowContexts)
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