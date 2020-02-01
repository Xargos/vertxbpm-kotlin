package process.performance

import io.vertx.core.AsyncResult
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.spi.cluster.ClusterManager
import io.vertx.spi.cluster.ignite.IgniteClusterManager
import org.apache.ignite.Ignition
import kotlin.system.exitProcess

fun main() {

    Ignition.start()
    val ignite = Ignition.ignite()
    val clusterManager: ClusterManager = IgniteClusterManager(ignite)

    val options = VertxOptions().setClusterManager(clusterManager)
    Vertx.clusteredVertx(options) { res: AsyncResult<Vertx?> ->
        if (res.succeeded()) {
            res.result()?.deployVerticle(PerformanceTestVerticle()) {
                if (it.failed()) {
                    println("Startup failed")
                    it.cause().printStackTrace()
                    exitProcess(-1)
                } else {
                    println("Test Finished")
                }
            }
        } else { // failed!
            throw res.cause()
        }
    }
}