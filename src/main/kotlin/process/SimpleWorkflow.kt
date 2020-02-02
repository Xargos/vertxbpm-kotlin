package process

import io.vertx.core.Future
import process.engine.Step
import process.engine.StepName
import process.engine.Workflow

fun simpleWorkflow(): Workflow<String> {
    return Workflow(
        name = "SimpleWorkflow",
        startNode = StepName("start"),
        steps = buildSteps(),
        decodeData = { it ?: "" })
}

private fun buildSteps(): Map<StepName, Step<String>> {
    return listOf(
        Step.Simple<String>(
            StepName("start"),
            StepName("step")
        ) {
            //                println("Start")
            Future.succeededFuture(it)
        },
        Step.Simple<String>(
            StepName("step"),
            StepName("end")
        ) { data ->
            //                println("Step")
//                val promise = Promise.promise<String>()
//                vertx.setTimer(20000) { promise.complete(data) }
//                promise.future()
//                Thread.sleep(1000)
            Future.succeededFuture(data)
        },
//            Step.Simple<String>(
//                StepName("step"),
//                StepName("end")
//            ) { data ->
//                println("Step")
////                if (Random.nextBoolean()) {
////                    throw RuntimeException("Ble")
////                }
//                Future.succeededFuture(data)
//            },
        Step.End<String>(StepName("end")) {
            //                println("End")
            Future.succeededFuture(it)
        }
    ).associateBy({ it.name }, { it })
}