package process

import io.vertx.core.Future
import io.vertx.core.Vertx
import process.engine.Step
import process.engine.StepName
import process.engine.Workflow
import kotlin.random.Random

class SimpleWorkflow : Workflow<String> {
    override fun name(): String {
        return "SimpleWorkflow"
    }

    override fun startNode(): StepName {
        return StepName("start")
    }

    override fun steps(vertx: Vertx): Map<StepName, Step<String>> {
        return listOf(
            Step.Start<String>(
                StepName("start"),
                StepName("step")
            ) { println("Start"); Future.succeededFuture(it) },
//            Step.Simple<String>(
//                StepName("step"),
//                StepName("end")
//            ) { data ->
//                println("Step")
//                throw RuntimeException("Ble")
//                val promise = Promise.promise<String>()
//                vertx.setTimer(20000) { promise.complete(data) }
//                promise.future()
//            },
            Step.Simple<String>(
                StepName("step"),
                StepName("end")
            ) { data ->
                println("Step")
                if (Random.nextBoolean()) {
                    throw RuntimeException("Ble")
                }
                Future.succeededFuture(data)
            },
            Step.End(StepName("end")) {
                println("End")
                Future.succeededFuture(it)
            }
        ).associateBy({ it.name }, { it })
    }

    override fun decodeData(data: String?): String {
        return data ?: ""
    }
}