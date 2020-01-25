package process.infrastructure

import io.vertx.core.Future
import io.vertx.core.Promise
import org.apache.ignite.Ignite
import org.apache.ignite.cache.query.ScanQuery
import org.apache.ignite.lang.IgniteBiPredicate
import process.engine.*
import javax.cache.Cache

// TODO: Optimize stored data structure. Maybe additional indexes for restarting jobs.
//  Make all async.
class IgniteRepository(
    private val processesCache: String,
    private val engineCache: String,
    private val ignite: Ignite
) : Repository {

    override fun <T> saveProcess(flowContext: FlowContext<T>, processId: ProcessId, engineId: EngineId): Future<Void> {
        try {
            val promise = Promise.promise<Void>()
            val engineCache = ignite.getOrCreateCache<EngineId, Set<ProcessId>>(engineCache)
            val processIds = engineCache.get(engineId)
            engineCache.putAsync(engineId, processIds.plus(processId))
            val processesCache = ignite.getOrCreateCache<ProcessId, FlowContext<T>?>(processesCache)
            processesCache.putAsync(processId, flowContext)
                .listen { promise.complete() }
            return promise.future()
        } catch (ex: Exception) {
            ex.printStackTrace()
            throw RuntimeException(ex)
        }
    }

    override fun <T> retrieveProcess(processId: ProcessId): Future<FlowContext<T>?> {
        try {
            val promise = Promise.promise<FlowContext<T>?>()
            val cache = ignite.getOrCreateCache<ProcessId, FlowContext<T>?>(processesCache)
            cache.getAsync(processId)
                .listen { promise.complete(it.get()) }
            return promise.future()
        } catch (ex: Exception) {
            ex.printStackTrace()
            throw RuntimeException(ex)
        }
    }

    override fun retrieveAllProcesses(): List<FlowContext<Any>> {
        try {
            val cache = ignite.getOrCreateCache<String, FlowContext<Any>>(processesCache)
            val allValues: IgniteBiPredicate<ProcessId, FlowContext<Any>>? = null
            return cache.query(ScanQuery(allValues)) { it.value }.all
        } catch (ex: Exception) {
            ex.printStackTrace()
            throw RuntimeException(ex)
        }
    }

    override fun getActiveProcesses(engineIds: Set<EngineId>): Future<List<FlowContext<Any>>> {
        val cache = ignite.getOrCreateCache<EngineId, Set<ProcessId>>(engineCache)

        cache.getAll(engineIds)
            .values
            .flatMap {
                it.s
            }

        val keys = cache.query<Cache.Entry<ProcessId, FlowContext<Any>>, Any>(
            ScanQuery<ProcessId, FlowContext<Any>>(
                IgniteBiPredicate<ProcessId, FlowContext<Any>> { _: ProcessId, fc: FlowContext<Any> -> fc.currentStep is Step.End<*> }
            ),  // Remote filter.
            Cache.Entry<ProcessId, FlowContext<Any>>::getKey // Transformer.
        ).all


        return Future.succeededFuture<List<FlowContext<Any>>>()
    }
}