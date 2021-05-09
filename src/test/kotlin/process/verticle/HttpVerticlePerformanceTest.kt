package process.verticle

import com.beust.klaxon.Klaxon
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.ext.web.client.WebClient
import io.vertx.junit5.Checkpoint
import io.vertx.junit5.Timeout
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
import java.util.concurrent.TimeUnit

@ExtendWith(VertxExtension::class)
class HttpVerticlePerformanceTest {

    @Test
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    fun `Given http verticle when 1000 dispatched processes then engine should execute them all 1 second after last one is dispatched`(
        vertx: Vertx,
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
        val numberOfProcesses = 1000

        startVerticle { clusterManager -> buildHttpVerticle(clusterManager, workflows) }
            .onFailure { testContext.failNow(it) }
            .onSuccess { clusteredVertx ->
                val webClient = WebClient.create(vertx)
                CompositeFuture.join((1..numberOfProcesses).map { dispatchProcesses(webClient, workflow.name) })
                    .onFailure { testContext.failNow(it) }
                    .onSuccess {
                        waitForProcessToFinish(
                            clusteredVertx,
                            testContext,
                            statisticsCorrect,
                            numberOfProcesses,
                            webClient
                        )
                    }
            }
    }

    @AfterEach
    fun stopIgnite() {
        Ignition.stop(false)
    }

    private fun dispatchProcesses(
        webClient: WebClient,
        workflowName: String
    ): Future<Void> {
        return webClient.post(8080, "localhost", "/workflow/${workflowName}")
            .putHeader("Content-Length", "0")
            .send()
            .map {
                if (it.statusCode() != 200) {
                    Exception(it.statusMessage())
                }
                null
            }
    }

    private fun waitForProcessToFinish(
        vertx: Vertx,
        testContext: VertxTestContext,
        statisticsCorrect: Checkpoint,
        numberOfProcesses: Int,
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
                        assertThat(statistics?.processCount).isEqualTo(numberOfProcesses)
                        assertThat(statistics?.finishedProcessesCount).isEqualTo(numberOfProcesses)
                        println("All done")
                        println(statistics)
                        statisticsCorrect.flag()
                    }
                }
                .onFailure { testContext.failNow(it) }
        }
    }
}