package process.infrastructure

import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import org.apache.ignite.Ignite
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.configuration.CollectionConfiguration
import process.engine.*
import javax.cache.Cache


// TODO: Optimize stored data structure. Maybe additional indexes for restarting jobs.
//  Make all async.
//  When cleaning up dead nodes remove also all the engines and processes mappings.!
class IgniteRepository(
    processesCacheName: String,
    finishedProcessesCacheName: String,
    nodesCacheName: String,
    waitProcessesQueueName: String,
    ignite: Ignite
) : Repository {

    private val transactions = ignite.transactions()
    private val processesCache = ignite.getOrCreateCache<ProcessId, FlowContext<Any>>(
        CacheConfiguration<ProcessId, FlowContext<Any>>()
            .setName(processesCacheName)
            .setBackups(2)
    )
    private val finishedProcessesCache =
        ignite.getOrCreateCache<ProcessId, FlowContext<Any>>(
            CacheConfiguration<ProcessId, FlowContext<Any>>()
                .setName(finishedProcessesCacheName)
                .setBackups(2)
        )
    private val nodesCache = ignite.getOrCreateCache<NodeId, Set<ProcessId>>(
        CacheConfiguration<NodeId, Set<ProcessId>>()
            .setName(nodesCacheName)
            .setBackups(2)
    )
    private val waitProcessesQueue = ignite.queue<FlowContext<Any>>(
        waitProcessesQueueName,
        0,
        CollectionConfiguration().setBackups(2)
    )
    private lateinit var nodeId: NodeId
    private lateinit var vertx: Vertx

    private val newProcesses: MutableList<Pair<ProcessId, Promise<Void>>> = mutableListOf()
    private val newContexts: MutableList<Pair<FlowContext<Any>, Promise<FlowContext<Any>>>> = mutableListOf()
    private val pendingProcessesToSave: MutableList<Pair<FlowContext<Any>, Promise<Void>>> = mutableListOf()
    private val pendingProcessesToFinish: MutableList<Pair<FlowContext<Any>, Promise<Void>>> = mutableListOf()

    override fun init(vertx: Vertx, nodeId: NodeId): Future<Void> {
        this.nodeId = nodeId
        this.vertx = vertx
        nodesCache.putIfAbsent(nodeId, setOf())
        runBatchUpload(vertx)
        return Future.succeededFuture()
    }

    private fun runBatchUpload(vertx: Vertx) {
        vertx.setTimer(10) {
            if (newProcesses.isNotEmpty()
                || newContexts.isNotEmpty()
                || pendingProcessesToSave.isNotEmpty()
                || pendingProcessesToFinish.isNotEmpty()
            ) {
                CompositeFuture.join(
                    assignNewProcessesToEngine(vertx),
                    runGetOrCreateProcess(vertx),
                    runStoreSteps(vertx),
                    runFinishProcess(vertx)
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

    private fun runFinishProcess(vertx: Vertx): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking<Void>({ p ->

            val pendingProcessesToEndLocal = pendingProcessesToFinish.toMap()
            pendingProcessesToFinish.clear()
            val finishedProcesses = pendingProcessesToEndLocal.keys.map { it.processId to it }.toMap()
            finishedProcessesCache.putAllAsync(finishedProcesses)
                .listen {
                    processesCache.removeAllAsync(pendingProcessesToEndLocal.keys.map { it.processId }.toSet())
                        .listen {
                            //                    println("Stored ${pendingProcessesToSaveLocal.size} processes")
                            pendingProcessesToEndLocal.values.forEach { it.complete() }
                            p.complete()
                        }
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

    override fun <T> finishProcess(flowContext: FlowContext<T>): Future<Void> {
        val promise = Promise.promise<Void>()
        pendingProcessesToFinish.add(
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

    override fun <T> getProcess(processId: ProcessId): Future<FlowContext<T>?> {
        val promise = Promise.promise<FlowContext<T>?>()
        when {
            finishedProcessesCache.containsKey(processId) -> {
                finishedProcessesCache.getAsync(processId).listen {
                    promise.complete(it.get() as FlowContext<T>)
                }
            }
            processesCache.containsKey(processId) -> {
                processesCache.getAsync(processId).listen {
                    promise.complete(it.get() as FlowContext<T>)
                }
            }
            else -> {
                promise.complete()
            }
        }
        return promise.future()
    }

    override fun retrieveAllProcesses(): Future<Set<FlowContext<Any>>> {
        val promise = Promise.promise<Set<FlowContext<Any>>>()
        vertx.executeBlocking<Set<FlowContext<Any>>>({ p ->
            val processes: Iterator<Cache.Entry<ProcessId, FlowContext<Any>>> = processesCache.iterator()

            val fc: MutableMap<ProcessId, FlowContext<Any>> = mutableMapOf()
            while (processes.hasNext()) {
                val process = processes.next()
                fc[process.key] = process.value
            }

            val finishedProcesses: Iterator<Cache.Entry<ProcessId, FlowContext<Any>>> =
                finishedProcessesCache.iterator()
            while (finishedProcesses.hasNext()) {
                val process = finishedProcesses.next()
                fc[process.key] =
                    process.value // Might have already been finished but had not yet been removed from processes
            }
            p.complete(fc.values.toSet())
        }) {
            if (it.failed()) {
                promise.fail(it.cause())
            } else {
                promise.complete(it.result())
            }
        }
        return promise.future()
    }

    override fun startNewProcess(processId: ProcessId): Future<Void> {
        val promise = Promise.promise<Void>()
        newProcesses.add(Pair(processId, promise))
        return promise.future()
    }

    override fun removeProcessFromEngine(processId: ProcessId): Future<Void> {
        val promise = Promise.promise<Void>()
        nodesCache.getAsync(nodeId)
            .listen {
                nodesCache.putAsync(nodeId, it.get()?.minus(processId) ?: mutableSetOf())
                    .listen { promise.complete() }
            }
        return promise.future()
    }

    override fun moveDeadNodeProcessesToWaitQueueAndCleanup(nodeId: NodeId): Future<Void> {
        val promise = Promise.promise<Void>()
        transactions.txStart().use { tx ->
            try {
                if (!nodesCache.containsKey(nodeId)) {
                    tx.rollback()
                    promise.complete()
                } else {
                    val processIds = nodesCache.get(nodeId)

                    println(processIds)

                    val flowContexts = processesCache.getAll(processIds).values
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
        return promise.future()
    }

    override fun getAndExecuteWaitingProcess(exec: (fc: FlowContext<Any>) -> Future<Void>): Future<Void> {
        val promise = Promise.promise<Void>()
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
        return promise.future()
    }
}