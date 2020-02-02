package process

import io.vertx.core.AsyncResult
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.spi.cluster.ClusterManager
import io.vertx.spi.cluster.ignite.IgniteClusterManager
import org.apache.ignite.Ignition
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.configuration.CacheConfiguration
import process.engine.Workflow
import process.verticles.buildEventBusVerticle
import kotlin.system.exitProcess


fun main() {

    val simpleWorkflow = simpleWorkflow()
    val workflows = mapOf(simpleWorkflow.name to (simpleWorkflow as Workflow<Any>))

    Ignition.start()
    val ignite = Ignition.ignite()
    val cacheConfiguration = CacheConfiguration<Any, Any>(IgniteClusterManager.VERTX_CACHE_TEMPLATE_NAME)
        .setBackups(2)
        .setCacheMode(CacheMode.REPLICATED)
//        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
    ignite.addCacheConfiguration(cacheConfiguration)
    val clusterManager: ClusterManager = IgniteClusterManager(ignite)

    val options = VertxOptions()
        .setClusterManager(clusterManager)
        .setHAEnabled(true)
    val deploymentOptions = DeploymentOptions()
        .setHa(true)
    Vertx.clusteredVertx(options) { res: AsyncResult<Vertx?> ->
        if (res.succeeded()) {
            (1..2).forEach {
                res.result()
                    ?.deployVerticle(buildEventBusVerticle(workflows, ignite, clusterManager), deploymentOptions) {
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

