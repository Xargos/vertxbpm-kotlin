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
const val speed = 10L
const val repetitions: Int = 100

class PerformanceTestVerticle(
) : AbstractVerticle() {

    var counter = 0
    override fun start(startPromise: Promise<Void>) {
        println("Startup finished successfully")
        var count = 1
        val startTime = System.nanoTime()
        val eventBus = vertx.eventBus()
        val deliveryOptions = DeliveryOptions()
        deliveryOptions.addHeader("workflowName", "SimpleWorkflow")
        (1..repetitions).forEach { id ->
            val start = System.nanoTime()
            CompositeFuture.join((1..jobNo).map {
                val promise = Promise.promise<Void>()
//                GlobalScope.launch {
                    counter++
                    eventBus.send("start_process", "", deliveryOptions)
//                    { response ->
//                        if (response.failed()) {
//                            promise.fail(response.cause())
//                        } else {
//                            promise.complete()
//                        }
//                    }
//                }
                promise.complete()
                promise.future()
            })
//                            .onSuccess { println("All succeeded: $jobNo") }
                .onFailure { println("Some failed") }
                .onComplete {
                    println("Duration (millis): ${(System.nanoTime() - start) / 1_000_000}")
                    if (count == repetitions) {
                        println("counter $counter")
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

        vertx.setTimer(1000) { periodic ->
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
                    } else {
                        check(vertx, startTime, startPromise)
                    }
                } else {
                    it.cause().printStackTrace()
                    check(vertx, startTime, startPromise)
                }
            }
        }
    }
}