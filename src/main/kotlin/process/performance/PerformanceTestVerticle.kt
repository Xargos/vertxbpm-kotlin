package process.performance

import com.beust.klaxon.Klaxon
import io.vertx.core.AbstractVerticle
import io.vertx.core.CompositeFuture
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.eventbus.DeliveryOptions
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import process.engine.Statistics

const val jobNo: Int = 100
const val speed = 500L
const val repetitions: Int = 10

class PerformanceTestVerticle(
) : AbstractVerticle() {

    override fun start(startPromise: Promise<Void>) {
        println("Startup finished successfully")
        var count = 1
        val startTime = System.nanoTime()
        val eventBus = vertx.eventBus()
        val deliveryOptions = DeliveryOptions()
        deliveryOptions.addHeader("workflowName", "SimpleWorkflow")
        vertx.setPeriodic(speed) { id ->
            val start = System.nanoTime()
            CompositeFuture.join((1..jobNo).map {
                val promise = Promise.promise<Void>()
                GlobalScope.launch {
                    eventBus.request<String>("start_process", "", deliveryOptions) { response ->
                        if (response.failed()) {
                            promise.fail(response.cause())
                        } else {
                            promise.complete()
                        }
                    }
                }
                promise.future()
            })
//                            .onSuccess { println("All succeeded: $jobNo") }
                .onFailure { println("Some failed") }
                .onComplete {
                    println("Duration (millis): ${(System.nanoTime() - start) / 1_000_000}")
                    if (count == repetitions) {
                        vertx.cancelTimer(id)
                        check(vertx, startTime, startPromise)
                    } else {
                        count++
                    }

                }
        }
    }

    private fun check(
        vertx: Vertx,
        startTime: Long,
        startPromise: Promise<Void>
    ) {
        val eventBus = vertx.eventBus()

        vertx.setPeriodic(1000) { periodic ->
            eventBus.request<String>("statistics", "") {
                if (it.succeeded()) {
                    val statistics = Klaxon().parse<Statistics>(it.result().body())
//                    val statistics = Json.decodeValue(it.result().body(), Statistics::class.java)
//            val statistics = it.result().body()
                    println("active: ${statistics?.activeProcessCount}")
                    println("total stored processes: ${statistics?.processCount}")
                    if (statistics?.activeProcessCount == 0) {
                        val duration = (System.nanoTime() - startTime) / 1_000_000_000
                        println("time took: $duration, per second: ${jobNo * repetitions / duration}, total processed jobs: ${statistics.finishedProcessesCount}")
                        vertx.cancelTimer(periodic)
                        startPromise.complete()
                    }
                } else {
                    it.cause().printStackTrace()
                }
            }
        }
    }
}