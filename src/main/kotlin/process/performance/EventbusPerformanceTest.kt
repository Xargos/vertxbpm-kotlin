package process.performance

import io.vertx.core.AsyncResult
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.spi.cluster.ClusterManager
import io.vertx.spi.cluster.ignite.IgniteClusterManager
import org.apache.ignite.Ignition
import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.configuration.IgniteConfiguration
import kotlin.system.exitProcess

fun main() {

//    Ignition.start()
//    val ignite = Ignition.ignite()
    val cacheConfiguration = IgniteConfiguration()
//    val cacheConfiguration = CacheConfiguration<Any, Any>(IgniteClusterManager.VERTX_CACHE_TEMPLATE_NAME)
//        .setBackups(2)
//        .setCacheMode(CacheMode.REPLICATED)
//        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
//    ignite.addCacheConfiguration(cacheConfiguration)
    val clusterManager: ClusterManager = IgniteClusterManager(cacheConfiguration)

    val options = VertxOptions().setClusterManager(clusterManager).setHAEnabled(true)
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