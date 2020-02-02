package process


//fun main() {
//
//    val simpleWorkflow = simpleWorkflow()
//    val workflows = mapOf(simpleWorkflow.name() to (simpleWorkflow as Workflow<Any>))
//
//    val clusterManager: ClusterManager = IgniteClusterManager()
//
//    val options = VertxOptions().setClusterManager(clusterManager)
//    Vertx.clusteredVertx(options) { res: AsyncResult<Vertx?> ->
//        if (res.succeeded()) {
//            val serverVerticle = buildControlVerticle(workflows)
//            res.result()?.deployVerticle(serverVerticle) {
//                if (it.failedApp()) {
//                    println("Startup failed")
//                    it.cause().printStackTrace()
//                    exitProcess(-1)
//                } else {
//                    println("Startup finished successfully")
//                }
//            }
////                ?.onSuccess {
////                    println("Startup finished successfully")
////                }
////                ?.onFailure {
////                    println("Startup failed")
////                    it.printStackTrace()
////                    exitProcess(-1)
////                }
//        } else { // failed!
//            throw res.cause()
//        }
//    }
//}

//fun main() {
//    val p1: Promise<Int> = Promise.promise()
//    val p2: Promise<Int> = Promise.promise()
//    CompositeFuture.all(p1.future(), p2.future())
//        .onFailure { t: Throwable -> t.printStackTrace() }
//        .onSuccess { cf: CompositeFuture? -> println("Completed") }
//    p1.complete(1)
//    p2.fail(RuntimeException("FAILED"))
//}

//private fun buildControlVerticle(workflows: Map<String, Workflow<Any>>): ControlVerticle {
//    val ulid = ULID()
//    Ignition.start()
//    val igniteRepository = IgniteRepository(
//        waitProcessesQueueName = "waitProcessesQueue",
//        processesCacheName = "processes",
//        enginesCacheName = "engines",
//        nodesCacheName = "nodes",
//        ignite = Ignition.ignite()
//    )
//    val processQueryService = ProcessQueryService(igniteRepository)
//    val workflowEngineFactory = WorkflowEngineFactory(igniteRepository)
//    val config = Config(2, 8080)
//    val engineHealthCheckService = EngineHealthCheckService(igniteRepository)
//    val engineService = EngineService(
//        engineHealthCheckService = engineHealthCheckService,
//        workflowEngineFactory = workflowEngineFactory,
//        workflowStore = WorkflowStore(workflows),
//        ulid = ulid
//    )
//    val nodeSynchronizationService = NodeSynchronizationService(igniteRepository)
//    return ControlVerticle(
//        engineHealthCheckService = engineHealthCheckService,
//        engineService = engineService,
//        processQueryService = processQueryService,
//        nodeSynchronizationService = nodeSynchronizationService,
//        config = config
//    )
//}