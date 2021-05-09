package process.verticle

import com.beust.klaxon.Klaxon
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Promise.promise
import io.vertx.core.Vertx
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.junit5.Checkpoint
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import org.apache.ignite.Ignition
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import process.engine.Statistics
import process.engine.Step
import process.engine.StepName
import process.startVerticle
import process.testWorkflow
import process.verticles.buildEventBusVerticle

@ExtendWith(VertxExtension::class)
class EventBusVerticlePerformanceTest {

    @Test
    fun `Given event bus verticle when 5000 dispatched processes then engine should execute them all 1 second after last one is dispatched`(
        testContext: VertxTestContext
    ) {
        val statisticsCorrect = testContext.checkpoint()
        val step: (data: String) -> Future<String> = { Future.succeededFuture(it) }
        val singleStep = Step.End<String>(StepName("start")) { step(it) }
        val workflow = testWorkflow(
            startNode = singleStep.name,
            steps = listOf(singleStep)
        )
        val workflows = mapOf(workflow.name to { workflow })
        val deliveryOptions = DeliveryOptions()
        deliveryOptions.addHeader("workflowName", workflow.name)
        val numberOfProcesses = 5000

        startVerticle { clusterManager -> buildEventBusVerticle(clusterManager, workflows) }
            .onFailure { testContext.failNow(it) }
            .onSuccess { clusteredVertx ->
                CompositeFuture.join((1..numberOfProcesses).map { dispatchProcesses(clusteredVertx, deliveryOptions) })
                    .onFailure { testContext.failNow(it) }
                    .onSuccess {
                        waitForProcessToFinish(
                            clusteredVertx,
                            testContext,
                            statisticsCorrect,
                            numberOfProcesses
                        )
                    }
            }
    }

    @AfterEach
    fun stopIgnite() {
        Ignition.stop(false)
    }

    private fun dispatchProcesses(
        clusteredVertx: Vertx,
        deliveryOptions: DeliveryOptions
    ): Future<Void> {
        val promise = promise<Void>()
        clusteredVertx.eventBus().request<String>("start_process", "", deliveryOptions) {
            if (it.succeeded()) {
                promise.complete()
            } else {
                promise.fail(it.cause())
            }
        }
        return promise.future()
    }

    private fun waitForProcessToFinish(
        vertx: Vertx,
        testContext: VertxTestContext,
        statisticsCorrect: Checkpoint,
        numberOfProcesses: Int
    ) {
        vertx.setTimer(1000) {
            vertx.eventBus().request<String>("statistics", "") {
                if (it.succeeded()) {
                    testContext.verify {
                        val statistics = Klaxon().parse<Statistics>(it.result().body())
                        assertThat(statistics?.activeProcessCount).isEqualTo(0)
                        assertThat(statistics?.processCount).isEqualTo(numberOfProcesses)
                        assertThat(statistics?.finishedProcessesCount).isEqualTo(numberOfProcesses)
                        println("All done")
                        println(statistics)
                        statisticsCorrect.flag()
                    }
                } else {
                    testContext.failNow(it.cause())
                }
            }
        }
    }
}