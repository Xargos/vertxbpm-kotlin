package process.engine

import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.vertx.core.Future
import io.vertx.junit5.Checkpoint
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(VertxExtension::class)
class EngineTest {

    private val processId = ProcessId(value = "Test")
    private val exception = Exception("Test")

    @Test
    fun `Given single succeeding step workflow engine should call the step and complete the process`(testContext: VertxTestContext) {
        // GIVEN
        val stepExecuted = testContext.checkpoint()
        val processCompleted = testContext.checkpoint()
        val processDispatched = testContext.checkpoint()
        val repository = testRepository<String>(processCompleted)
        val workflow = testWorkflow {
            stepExecuted.flag()
            Future.succeededFuture(it)
        }
        val engine = Engine(repository)
        val emptyData = ""

        // WHEN
        engine.start(workflow, emptyData, processId)
            .onSuccess { processDispatched.flag() }
            .onFailure { testContext.failNow(it) }
    }

    @Test
    fun `Given single failing step workflow engine should call the step and complete the process storing the exception`(
        testContext: VertxTestContext
    ) {
        // GIVEN
        val stepExecuted = testContext.checkpoint()
        val processCompleted = testContext.checkpoint()
        val exceptionRecorded = testContext.checkpoint()
        val processDispatched = testContext.checkpoint()
        val repository = testRepository<String>(processCompleted)
        val slot = slot<FlowContext<String>>()
        every { repository.saveProcess(flowContext = capture(slot)) } answers {
            if (slot.captured.ended) {
                testContext.verify {
                    assertThat(slot.captured.exception).isNotNull().isEqualTo(exception)
                    exceptionRecorded.flag()
                }
            }
            Future.succeededFuture()
        }
        val workflow = testWorkflow {
            stepExecuted.flag()
            throw exception
        }
        val engine = Engine(repository)
        val emptyData = ""

        // WHEN
        engine.start(workflow, emptyData, processId)
            .onSuccess { processDispatched.flag() }
            .onFailure { testContext.failNow(it) }
    }

    @Test
    fun `Given multiple succeeding step workflow engine should call each step and complete the process`(testContext: VertxTestContext) {
        // GIVEN
        val stepsExecuted = testContext.checkpoint(3)
        val processCompleted = testContext.checkpoint()
        val processDispatched = testContext.checkpoint()
        val repository = testRepository<String>(processCompleted)
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
        val engine = Engine(repository)
        val emptyData = ""

        // WHEN
        engine.start(workflow, emptyData, processId)
            .onSuccess { processDispatched.flag() }
            .onFailure { testContext.failNow(it) }
    }

    private fun <T> testRepository(processCompleted: Checkpoint): Repository {
        val repository = mockk<Repository>()
        every { repository.startNewProcess(processId) } returns Future.succeededFuture()
        val slot = slot<FlowContext<T>>()
        every { repository.getOrCreateProcess(flowContext = capture(slot)) } answers { Future.succeededFuture(slot.captured) }
        every { repository.saveProcess(any<FlowContext<T>>()) } answers { Future.succeededFuture() }
        every { repository.removeProcessFromEngine(processId) } answers { processCompleted.flag(); Future.succeededFuture() }
        return repository
    }

    private fun testWorkflow(step: (data: String) -> Future<String>): Workflow<String> {
        val singleStep = Step.End<String>(StepName("start")) { step.invoke(it) }
        return testWorkflow(
            startNode = singleStep.name,
            steps = listOf(singleStep)
        )
    }

    private fun testWorkflow(startNode: StepName, steps: List<Step<String>>): Workflow<String> {
        return Workflow(
            name = "SimpleWorkflow",
            startNode = startNode,
            steps = steps.associateBy({ it.name }, { it }),
            decodeData = { it ?: "" })
    }
}