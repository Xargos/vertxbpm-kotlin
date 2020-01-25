package process

import de.huxhorn.sulky.ulid.ULID
import io.vertx.core.*
import io.vertx.core.spi.cluster.ClusterManager
import io.vertx.spi.cluster.ignite.IgniteClusterManager
import org.apache.ignite.Ignition
import process.control.*
import process.engine.*
import process.infrastructure.IgniteRepository
import kotlin.system.exitProcess


fun main() {

    val simpleWorkflow = SimpleWorkflow()
    val workflows = mapOf(simpleWorkflow.name() to (simpleWorkflow as Workflow<Any>))

    val clusterManager: ClusterManager = IgniteClusterManager()

    val options = VertxOptions().setClusterManager(clusterManager)
    Vertx.clusteredVertx(options) { res: AsyncResult<Vertx?> ->
        if (res.succeeded()) {
            val serverVerticle = buildControlVerticle(workflows)
            res.result()?.deployVerticle(serverVerticle) {
                if(it.failed()) {
                    println("Startup failed")
                    it.cause().printStackTrace()
                    exitProcess(-1)
                } else {
                    println("Startup finished successfully")
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

//fun main() {
//    val p1: Promise<Int> = Promise.promise()
//    val p2: Promise<Int> = Promise.promise()
//    CompositeFuture.all(p1.future(), p2.future())
//        .onFailure { t: Throwable -> t.printStackTrace() }
//        .onSuccess { cf: CompositeFuture? -> println("Completed") }
//    p1.complete(1)
//    p2.fail(RuntimeException("FAILED"))
//}

private fun buildControlVerticle(workflows: Map<String, Workflow<Any>>): ControlVerticle {
    Ignition.start()
    val igniteRepository = IgniteRepository("processes", "engines", Ignition.ignite())
    val processQueryService = ProcessQueryService(igniteRepository)
    val workflowEngineFactory = WorkflowEngineFactory(igniteRepository)
    val ulid = ULID()
    val config = Config(2)
    val engineHealthCheckService = EngineHealthCheckService(igniteRepository)
    val engineService = EngineService(
        engineHealthCheckService,
        workflowEngineFactory,
        WorkflowStore(workflows),
        NodeId(ulid.nextULID()),
        ulid
    )
    return ControlVerticle(
        engineHealthCheckService,
        engineService,
        processQueryService,
        config
    )
}