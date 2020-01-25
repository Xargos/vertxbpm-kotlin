package process.engine

import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.ext.web.RoutingContext

class SimpleWorkflow : Workflow<String> {
    override fun name(): String {
        return "SimpleWorkflow"
    }

    override fun startNode(): StepName {
        return StepName("start")
    }

    override fun steps(vertx: Vertx): Map<StepName, Step<String>> {
        return listOf(
            Step.Start<String>(StepName("start"), StepName("step")) { println("Start"); Future.succeededFuture(it) },
            Step.Simple<String>(StepName("step"), StepName("end")) { data ->
                println("Step")
                val promise = Promise.promise<String>()
                vertx.setTimer(2000) { promise.complete(data) }
                promise.future()
            },
            Step.End(StepName("end")) { println("End"); Future.succeededFuture(it) }
        ).associateBy({ it.name }, { it })
    }

    override fun decodeData(data: String?): String {
        return data ?: ""
    }
}