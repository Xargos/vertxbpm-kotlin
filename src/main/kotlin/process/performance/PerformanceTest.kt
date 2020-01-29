package process.performance

import de.huxhorn.sulky.ulid.ULID
import io.vertx.core.*
import io.vertx.core.spi.cluster.ClusterManager
import io.vertx.spi.cluster.ignite.IgniteClusterManager
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.ignite.Ignition
import process.SimpleWorkflow
import process.control.*
import process.engine.EngineId
import process.engine.Workflow
import process.engine.WorkflowEngineFactory
import process.engine.WorkflowStore
import process.infrastructure.IgniteRepository
import kotlin.system.exitProcess

const val jobNo: Int = 1000
const val speed = 100L
const val repetitions: Int = 200

fun main() {

    val simpleWorkflow = SimpleWorkflow()
    val workflows = mapOf(simpleWorkflow.name() to (simpleWorkflow as Workflow<Any>))

    val clusterManager: ClusterManager = IgniteClusterManager()

    val options = VertxOptions().setClusterManager(clusterManager)
    Vertx.clusteredVertx(options) { res: AsyncResult<Vertx?> ->
        if (res.succeeded()) {
            val serverVerticle = buildControlVerticle(workflows)
            res.result()?.deployVerticle(serverVerticle) {
                if (it.failed()) {
                    println("Startup failed")
                    it.cause().printStackTrace()
                    exitProcess(-1)
                } else {
                    println("Startup finished successfully")
                    val vertx = res.result()!!
                    var count = 0
                    val startTime = System.nanoTime()
                    vertx.setPeriodic(speed) {id ->
                        val start = System.nanoTime()
                        CompositeFuture.join((1..jobNo).map {
                            val promise = Promise.promise<Void>()
                            GlobalScope.launch {
                                try {
                                    serverVerticle.startProcess("SimpleWorkflow", "", vertx.eventBus())
                                        .onSuccess { promise.complete() }
                                        .onFailure { t -> promise.fail(t.cause) }
                                } catch (e: Exception) {
                                    promise.fail(e)
                                }
                            }
                            promise.future()
                        })
//                            .onSuccess { println("All succeeded: $jobNo") }
                            .onFailure { println("Some failed") }
                            .onComplete {
                                println("Duration (millis): ${(System.nanoTime() - start) / 1_000_000}")
                                if(count == repetitions) {
                                    vertx.cancelTimer(id)
                                    check(vertx, serverVerticle, startTime)
                                } else {
                                    count++
                                }

                            }
                    }
                }
            }
//                ?.onSuccess {
//                    println("Startup finished successfully")
//                }
//                ?.onFailure {
//                    println("Startup failed")
//                    it.printStackTrace()
//                    exitProcess(-1)
//                }
        } else { // failed!
            throw res.cause()
        }
    }
}

private fun check(vertx: Vertx, serverVerticle: PerformanceTestVerticle, startTime: Long) {
    vertx.setPeriodic(1000) { periodic ->
        serverVerticle.getProcessCount()
            .setHandler { println("total stored processes: $it") }
        serverVerticle.getActiveProcessesCount()
            .setHandler {
                println("active: $it")

                if (it.result() == 0) {
                    val duration = (System.nanoTime() - startTime) / 1_000_000_000
                    println("time took: $duration, per second: ${jobNo / duration}, total processed jobs: ${serverVerticle.getTotalFinishedProcesses()}")
                    vertx.cancelTimer(periodic)
                }
            }
    }
}

//fun main() {
//    val p1: Promise<Int> = Promise.promise()
//    val p2: Promise<Int> = Promise.promise()
//    CompositeFuture.all(p1.future(), p2.future())
//        .onFailure { t: Throwable -> t.printStackTrace() }
//        .onSuccess { cf: CompositeFuture? -> println("Completed") }
//    p1.complete(1)
//    p2.fail(RuntimeException("FAILED"))
//}

private fun buildControlVerticle(workflows: Map<String, Workflow<Any>>): PerformanceTestVerticle {
    val ulid = ULID()
    Ignition.start()
    val engineId = EngineId(ulid.nextULID().toString())
    val igniteRepository = IgniteRepository(
        engineId = engineId,
        waitProcessesQueueName = "waitProcessesQueue",
        processesCacheName = "processes",
        enginesCacheName = "engines",
        nodesCacheName = "nodes",
        ignite = Ignition.ignite()
    )
    val processQueryService = ProcessQueryService(igniteRepository)
    val workflowEngineFactory = WorkflowEngineFactory(igniteRepository)
    val config = Config(1, 8080)
    val engineHealthCheckService = EngineHealthCheckService(igniteRepository)
    val engineService = EngineService(
        engineHealthCheckService = engineHealthCheckService,
        workflowEngineFactory = workflowEngineFactory,
        workflowStore = WorkflowStore(workflows),
        ulid = ulid
    )
    val nodeSynchronizationService = NodeSynchronizationService(igniteRepository)
    return PerformanceTestVerticle(
        engineService = engineService,
        processQueryService = processQueryService,
        repository = igniteRepository
    )
}