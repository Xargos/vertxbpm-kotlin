package process.engine

import de.huxhorn.sulky.ulid.ULID
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.spi.cluster.ClusterManager
import io.vertx.junit5.Checkpoint
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import process.testWorkflow

@ExtendWith(VertxExtension::class)
class EngineServiceTest {

    @Test
    fun `Given multiple succeeding step workflow then engine service should execute workflow`(
        vertx: Vertx,
        testContext: VertxTestContext
    ) {
        // GIVEN
        val stepsExecuted = testContext.checkpoint(3)
        val processCompleted = testContext.checkpoint(2)
        val processDispatched = testContext.checkpoint()
        val nodeId = NodeId("NodeId")
        val repository = testRepository<String>(vertx, nodeId, processCompleted)
        val step: (data: String) -> Future<String> = {
            stepsExecuted.flag()
            Future.succeededFuture(it)
        }
        val startStep = Step.Simple<String>(StepName("start"), StepName("step")) { step.invoke(it) }
        val workflow = testWorkflow(
            startNode = startStep.name,
            steps = listOf(startStep,
                Step.Simple(StepName("step"), StepName("end")) { step.invoke(it) },
                Step.End(StepName("end")) { step.invoke(it) })
        )
        val engineService = EngineService(
            engine = Engine(repository),
            workflowStore = WorkflowStore(mapOf(Pair(workflow.name, workflow))),
            nodeSynchronizationService = NodeSynchronizationService(repository),
            ulid = ULID(),
            repository = repository,
            clusterManager = clusterManager()
        )
        engineService.start(vertx)
        val emptyData = ""

        // WHEN
        engineService.startProcess(workflow.name, emptyData)
            .onSuccess { processDispatched.flag() }
            .onFailure { testContext.failNow(it) }
    }

    private fun clusterManager(): ClusterManager {
        val clusterManager = mockk<ClusterManager>()
        every { clusterManager.nodeID } returns "NodeId"
        every { clusterManager.nodeListener(any()) } answers {}
        return clusterManager
    }

    private fun <T> testRepository(vertx: Vertx, nodeId: NodeId, processCompleted: Checkpoint): Repository {
        val repository = mockk<Repository>()
        every { repository.init(vertx, nodeId) } returns Future.succeededFuture()
        every { repository.startNewProcess(any()) } returns Future.succeededFuture()
        val slot = slot<FlowContext<T>>()
        every { repository.getOrCreateProcess(flowContext = capture(slot)) } answers { Future.succeededFuture(slot.captured) }
        every { repository.saveProcess(any<FlowContext<T>>()) } answers { Future.succeededFuture() }
        every { repository.finishProcess(any<FlowContext<T>>()) } answers { processCompleted.flag(); Future.succeededFuture() }
        every { repository.removeProcessFromEngine(any()) } answers { processCompleted.flag(); Future.succeededFuture() }
        return repository
    }
}