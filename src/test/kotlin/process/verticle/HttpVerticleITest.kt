package process.verticle

import com.beust.klaxon.Klaxon
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.http.HttpClient
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
        val workflows = mapOf(Pair(workflow.name, workflow))

        startVerticle { ignite, clusterManager -> buildHttpVerticle(workflows, ignite, clusterManager) }
            .onFailure { testContext.failNow(it) }
            .onSuccess { clusteredVertx ->
                val httpClient = vertx.createHttpClient()
                httpClient.post(8080, "localhost", "/workflow/${workflow.name}")
                {
                    if (it.statusCode() != 200) {
                        testContext.failNow(Exception(it.statusMessage()))
                    } else {
                        waitForProcessToFinish(clusteredVertx, testContext, statisticsCorrect, httpClient)
                    }
                }
                    .putHeader("Content-Length", "0")
                    .write("")
                    .end()
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
        httpClient: HttpClient
    ) {
        vertx.setTimer(1000) {
            httpClient.get(8080, "localhost", "/statistics/")
            {
                if (it.statusCode() != 200) {
                    testContext.failNow(Exception(it.statusMessage()))
                } else {
                    it.bodyHandler { buffer ->
                        val statistics = Klaxon().parse<Statistics>(buffer.toString())
                        assertThat(statistics?.activeProcessCount).isEqualTo(0)
                        assertThat(statistics?.processCount).isEqualTo(1)
                        assertThat(statistics?.finishedProcessesCount).isEqualTo(1)
                        statisticsCorrect.flag()
                    }
                }
            }
                .end()
        }
    }
}