package process.verticle

import com.beust.klaxon.Klaxon
import io.vertx.core.Future
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

@ExtendWith(VertxExtension::class)
class EventBusVerticleITest {

    @Test
    fun `Given event bus verticle engine when sent workflow then execute it`(
        vertx: Vertx,
        testContext: VertxTestContext
    ) {
        val stepsExecuted = testContext.checkpoint(3)
        val statisticsCorrect = testContext.checkpoint()
        val step: (data: String) -> Future<String> = {
            stepsExecuted.flag()
            Future.succeededFuture(it)
        }
        val startStep = Step.Standard<String>(StepName("start"), StepName("step")) { step(it) }
        val workflow = testWorkflow(
            startNode = startStep.name,
            steps = listOf(startStep,
                Step.Standard(StepName("step"), StepName("end")) { step(it) },
                Step.End(StepName("end")) { step(it) })
        )
        val workflows = mapOf(Pair(workflow.name, workflow))
        val deliveryOptions = DeliveryOptions()
        deliveryOptions.addHeader("workflowName", workflow.name)

        startVerticle(workflows)
            .onFailure { testContext.failNow(it) }
            .onSuccess { clusteredVertx ->
                clusteredVertx.eventBus().request<String>("start_process", "", deliveryOptions) {
                    if (it.failed()) {
                        testContext.failNow(it.cause())
                    } else {
                        waitForProcessToFinish(clusteredVertx, testContext, statisticsCorrect)
                    }
                }
            }
    }

    @AfterEach
    fun stopIgnite() {
        Ignition.stop(false)
    }

    private fun waitForProcessToFinish(
        vertx: Vertx,
        testContext: VertxTestContext,
        statisticsCorrect: Checkpoint
    ) {
        vertx.setTimer(1000) {
            vertx.eventBus().request<String>("statistics", "") {
                if (it.succeeded()) {
                    testContext.verify {
                        val statistics = Klaxon().parse<Statistics>(it.result().body())
                        assertThat(statistics?.activeProcessCount).isEqualTo(0)
                        assertThat(statistics?.processCount).isEqualTo(1)
                        assertThat(statistics?.finishedProcessesCount).isEqualTo(1)
                        statisticsCorrect.flag()
                    }
                } else {
                    testContext.failNow(it.cause())
                }
            }
        }
    }
}