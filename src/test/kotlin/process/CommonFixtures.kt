package process

import io.vertx.core.*
import io.vertx.spi.cluster.ignite.IgniteClusterManager
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration
import process.engine.LongWorkflow
import process.engine.Step
import process.engine.StepName


fun testWorkflow(startNode: StepName, steps: List<Step<String>>): LongWorkflow<Any> {
    return LongWorkflow(
        name = "SimpleWorkflow",
        startNode = startNode,
        steps = steps.associateBy({ it.name }, { it as Step<Any> }),
        decodeData = { it ?: "" })
}

fun startVerticle(
    verticle: (IgniteClusterManager) -> AbstractVerticle
): Future<Vertx> {
    val verticleStarted = Promise.promise<Vertx>()
    Ignition.start(IgniteConfiguration())
    val ignite = Ignition.ignite()
    val clusterManager = IgniteClusterManager(ignite)
    val options = VertxOptions()
        .setClusterManager(clusterManager)
    Vertx.clusteredVertx(options) { res: AsyncResult<Vertx?> ->
        if (res.succeeded()) {
            val vertx = res.result();

            vertx?.deployVerticle(verticle(clusterManager)) {
                if (it.failed()) {
                    verticleStarted.fail(it.cause())
                } else {
                    verticleStarted.complete(vertx)
                }
            }
        } else { // failed!
            verticleStarted.fail(res.cause())
        }
    }
    return verticleStarted.future()
}