package process

import io.vertx.core.*
import io.vertx.core.spi.cluster.ClusterManager
import io.vertx.spi.cluster.ignite.IgniteClusterManager
import org.apache.ignite.Ignite
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration
import process.engine.Step
import process.engine.StepName
import process.engine.Workflow


fun testWorkflow(startNode: StepName, steps: List<Step<String>>): Workflow<Any> {
    return Workflow(
        name = "SimpleWorkflow",
        startNode = startNode,
        steps = steps.associateBy({ it.name }, { it as Step<Any> }),
        decodeData = { it ?: "" })
}

fun startVerticle(
    verticle: (Ignite, ClusterManager) -> AbstractVerticle
): Future<Vertx> {
    val verticleStarted = Promise.promise<Vertx>()
    Ignition.start(IgniteConfiguration())
    val ignite = Ignition.ignite()
    val clusterManager: ClusterManager = IgniteClusterManager(ignite)
    val options = VertxOptions()
        .setClusterManager(clusterManager)
    Vertx.clusteredVertx(options) { res: AsyncResult<Vertx?> ->
        if (res.succeeded()) {
            val vertx = res.result();

            vertx?.deployVerticle(verticle(ignite, clusterManager)) {
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