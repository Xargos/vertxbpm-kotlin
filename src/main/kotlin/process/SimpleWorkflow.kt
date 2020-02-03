package process

import io.vertx.core.Future
import process.engine.Step
import process.engine.StepName
import process.engine.Workflow

fun simpleWorkflow(): Workflow<String> {
    return Workflow(
        name = "SimpleWorkflow",
        startNode = StepName("start"),
        steps = buildSteps().associateBy({ it.name }, { it }),
        decodeData = { it ?: "" })
}

private fun singleStep(): List<Step<String>> {
    return listOf(
        Step.End(
            StepName("start")
        ) {
            Future.succeededFuture(it)
        })
}

private fun buildSteps(): List<Step<String>> {
    return listOf(
        Step.Standard(
            StepName("start"),
            StepName("step")
        ) {
            Future.succeededFuture(it)
        },
        Step.Standard(
            StepName("step"),
            StepName("end")
        ) { data ->
            Future.succeededFuture(data)
        },
        Step.End(StepName("end")) {
            Future.succeededFuture(it)
        }
    )
}