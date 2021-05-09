package process.verticle

import com.beust.klaxon.Klaxon
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.ext.web.client.WebClient
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
import process.verticles.buildHttpVerticle

@ExtendWith(VertxExtension::class)
class HttpVerticleITest {

    @Test
    fun `Given http verticle engine when sent workflow post request then execute it`(
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
        val workflows = mapOf(workflow.name to { workflow })

        startVerticle { clusterManager -> buildHttpVerticle(clusterManager, workflows) }
            .onFailure { testContext.failNow(it) }
            .onSuccess { clusteredVertx ->
                val webClient = WebClient.create(vertx)
                webClient.post(8080, "localhost", "/workflow/${workflow.name}")
                    .putHeader("Content-Length", "0")
                    .send()
                    .onSuccess {
                        if (it.statusCode() != 200) {
                            testContext.failNow(Exception(it.statusMessage()))
                        } else {
                            waitForProcessToFinish(clusteredVertx, testContext, statisticsCorrect, webClient)
                        }
                    }
                    .onFailure { testContext.failNow(it) }
            }
    }

    @AfterEach
    fun stopIgnite() {
        Ignition.stop(false)
    }

    private fun waitForProcessToFinish(
        vertx: Vertx,
        testContext: VertxTestContext,
        statisticsCorrect: Checkpoint,
        webClient: WebClient
    ) {
        vertx.setTimer(1000) {
            webClient.get(8080, "localhost", "/statistics/")
                .send()
                .onSuccess {
                    if (it.statusCode() != 200) {
                        testContext.failNow(Exception(it.statusMessage()))
                    } else {
                        val statistics = Klaxon().parse<Statistics>(it.bodyAsString())
                        assertThat(statistics?.activeProcessCount).isEqualTo(0)
                        assertThat(statistics?.processCount).isEqualTo(1)
                        assertThat(statistics?.finishedProcessesCount).isEqualTo(1)
                        statisticsCorrect.flag()
                    }
                }
                .onFailure { testContext.failNow(it) }
        }
    }
}