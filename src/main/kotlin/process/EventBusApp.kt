package process

import process.engine.LongWorkflow
import io.vertx.core.AsyncResult
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.spi.cluster.ignite.IgniteClusterManager
import org.apache.ignite.configuration.IgniteConfiguration
import process.verticles.buildEventBusVerticle
import kotlin.system.exitProcess


fun main() {

    val simpleWorkflow = simpleWorkflow()
    val workflows = mapOf(simpleWorkflow.name to ({simpleWorkflow as LongWorkflow<Any> }))

//    Ignition.start()
//    val ignite = Ignition.ignite()
    val cacheConfiguration = IgniteConfiguration()
//        .setBackups(2)
//        .setCacheMode(CacheMode.REPLICATED)
//        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
//    ignite.addCacheConfiguration(cacheConfiguration)
    val clusterManager = IgniteClusterManager(cacheConfiguration)

    val options = VertxOptions()
        .setClusterManager(clusterManager)
        .setHAEnabled(true)
    val deploymentOptions = DeploymentOptions()
        .setHa(true)
    Vertx.clusteredVertx(options) { res: AsyncResult<Vertx?> ->
        if (res.succeeded()) {
            (1..2).forEach { _ ->
                res.result()
                    ?.deployVerticle(
                        buildEventBusVerticle(
                            longWorkflows = workflows,
                            clusterManager = clusterManager
                        ),
                        deploymentOptions
                    ) {
                        if (it.failed()) {
                            println("Startup failed")
                            it.cause().printStackTrace()
                            exitProcess(-1)
                        } else {
                            println("Startup finished successfully")
                        }
                    }
            }
        } else { // failed!
            throw res.cause()
        }
    }
}

