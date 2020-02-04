package process.verticle

import com.beust.klaxon.Klaxon
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Promise
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
class HttpVerticlePerformanceTest {

    @Test
    fun `Given http verticle when 5000 dispatched processes then engine should execute them all 1 second after last one is dispatched`(
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
        val workflows = mapOf(Pair(workflow.name, workflow))
        val deliveryOptions = DeliveryOptions()
        deliveryOptions.addHeader("workflowName", workflow.name)
        val numberOfProcesses = 5000

        startVerticle { ignite, clusterManager -> buildHttpVerticle(workflows, ignite, clusterManager) }
            .onFailure { testContext.failNow(it) }
            .onSuccess { clusteredVertx ->
                val httpClient = vertx.createHttpClient()
                CompositeFuture.join((1..numberOfProcesses).map { dispatchProcesses(httpClient, workflow.name) })
                    .onFailure { testContext.failNow(it) }
                    .onSuccess {
                        waitForProcessToFinish(
                            clusteredVertx,
                            testContext,
                            statisticsCorrect,
                            numberOfProcesses,
                            httpClient
                        )
                    }
            }
    }

    @AfterEach
    fun stopIgnite() {
        Ignition.stop(false)
    }

    private fun dispatchProcesses(
        httpClient: HttpClient,
        workflowName: String
    ): Future<Void> {
        val promise = Promise.promise<Void>()
        httpClient.post(8080, "localhost", "/workflow/${workflowName}")
        {
            if (it.statusCode() != 200) {
                promise.fail(Exception(it.statusMessage()))
            } else {
                promise.complete()
            }
        }
            .putHeader("Content-Length", "0")
            .write("")
            .end()
        return promise.future()
    }

    private fun waitForProcessToFinish(
        vertx: Vertx,
        testContext: VertxTestContext,
        statisticsCorrect: Checkpoint,
        numberOfProcesses: Int,
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
                        assertThat(statistics?.processCount).isEqualTo(numberOfProcesses)
                        assertThat(statistics?.finishedProcessesCount).isEqualTo(numberOfProcesses)
                        println("All done")
                        println(statistics)
                        statisticsCorrect.flag()
                    }
                }
            }
                .end()
        }
    }
}