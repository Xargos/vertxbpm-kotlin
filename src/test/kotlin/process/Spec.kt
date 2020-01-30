package process

import io.mockk.every
import io.mockk.mockk
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import org.junit.jupiter.api.Assertions.assertEquals
import process.engine.Repository
import process.engine.Workflow
import process.verticles.ControlVerticle


class FirstSpec : Spek({
    val workflow = mockk<Workflow<String>>()
    val repository = mockk<Repository>()

    fun setup(buildControlVerticle: (workflows: Map<String, Workflow<Any>>) -> ControlVerticle) {
//
//        val workflows = mapOf(workflow.name() to (workflow as Workflow<Any>))
//
//        val clusterManager: ClusterManager = IgniteClusterManager()
//
//        val options = VertxOptions().setClusterManager(clusterManager)
//        Vertx.clusteredVertx(options) { res: AsyncResult<Vertx?> ->
//            if (res.succeeded()) {
//                val serverVerticle = buildControlVerticle(workflows)
//                res.result()?.deployVerticle(serverVerticle) {
//                    if (it.failed()) {
//                        println("Startup failed")
//                        it.cause().printStackTrace()
//                        exitProcess(-1)
//                    } else {
//                        println("Startup finished successfully")
//                    }
//                }
//            } else { // failed!
//                throw res.cause()
//            }
//        }
    }

//    fun buildTestControlVerticle(workflows: Map<String, Workflow<Any>>): ControlVerticle {
//        val ulid = ULID()
//        val nodeId = NodeId(ulid.nextULID())
//        val processQueryService = ProcessQueryService(repository)
//        val workflowEngineFactory = WorkflowEngineFactory(repository)
//        val config = Config(1, 8080)
//        val engineHealthCheckService = EngineHealthCheckService(repository)
//        val engineService = EngineService(
//            engineHealthCheckService = engineHealthCheckService,
//            workflowEngineFactory = workflowEngineFactory,
//            workflowStore = WorkflowStore(workflows),
//            nodeId = nodeId,
//            ulid = ulid
//        )
//        val nodeSynchronizationService = NodeSynchronizationService(repository, nodeId)
//        return ControlVerticle(
//            nodeId = nodeId,
//            engineHealthCheckService = engineHealthCheckService,
//            engineService = engineService,
//            processQueryService = processQueryService,
//            nodeSynchronizationService = nodeSynchronizationService,
//            config = config
//        )
//    }
    describe("Node") {
        beforeGroup {
            every { workflow.name() } returns "Test workflow"
//            setup { buildTestControlVerticle(it) }
        }
        afterGroup {
//            setup { buildTestControlVerticle(it) }
        }

        given("A calculator") {
            val calculator = 1
            on("Adding 3 and 5") {
                val result = 2
                it("Produces 8") {
                    assertEquals(3, result)
                    println("ello")
                }
            }
        }
    }
})