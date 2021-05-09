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
import process.testWorkflow

@ExtendWith(VertxExtension::class)
class EngineTest {

    private val processId = ProcessId(value = "Test")

    @Test
    fun `Given missing step in workflow then engine should throw exception and finish process`(testContext: VertxTestContext) {
        // GIVEN
        val processCompleted = testContext.checkpoint(2)
        val processDispatched = testContext.checkpoint()
        val exceptionRecorded = testContext.checkpoint()
        val repository = testRepository<String>(processCompleted)
        val slot = slot<FlowContext<String>>()
        every { repository.finishProcess(flowContext = capture(slot)) } answers {
            if (slot.captured.ended) {
                testContext.verify {
                    assertThat(slot.captured.exception).isNotNull().isInstanceOfAny(RuntimeException::class.java)
                    exceptionRecorded.flag()
                }
            }
            processCompleted.flag()
            Future.succeededFuture()
        }
        val singleStep = Step.End<String>(StepName("start")) { Future.succeededFuture() }
        val workflow = testWorkflow(
            startNode = StepName("missing step"),
            steps = listOf(singleStep)
        )
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
        val processCompleted = testContext.checkpoint(2)
        val exceptionRecorded = testContext.checkpoint()
        val processDispatched = testContext.checkpoint()
        val repository = testRepository<String>(processCompleted)
        val slot = slot<FlowContext<String>>()
        val exception = Exception("Test")
        every { repository.finishProcess(flowContext = capture(slot)) } answers {
            if (slot.captured.ended) {
                testContext.verify {
                    assertThat(slot.captured.exception).isNotNull().isEqualTo(exception)
                    exceptionRecorded.flag()
                }
            }
            processCompleted.flag()
            Future.succeededFuture()
        }
        val workflow = singleStepWorkflow {
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
        val stepsExecuted = testContext.checkpoint(4)
        val processCompleted = testContext.checkpoint(2)
        val processDispatched = testContext.checkpoint()
        val repository = testRepository<String>(processCompleted)
        val step: (data: String) -> Future<String> = {
            stepsExecuted.flag()
            Future.succeededFuture(it)
        }
        val startStep = Step.Standard<String>(StepName("start"), StepName("step1")) { step(it) }
        val workflow = testWorkflow(
            startNode = startStep.name,
            steps = listOf(startStep,
                Step.Choice(StepName("step1"), setOf(StepName("step2"), StepName("step3"))) {
                    stepsExecuted.flag()
                    Future.succeededFuture(StepName("step2"))
                },
                Step.Standard(StepName("step2"), StepName("end")) { step(it) },
                Step.Standard(StepName("step3"), StepName("end")) { step(it) },
                Step.End(StepName("end")) { step(it) })
        )
        val engine = Engine(repository)
        val emptyData = ""

        // WHEN
        engine.start(workflow, emptyData, processId)
            .onSuccess { processDispatched.flag() }
            .onFailure { testContext.failNow(it) }
    }

    @Test
    fun `Given workflow with no save step then workflow should execute but state should not be saved`(testContext: VertxTestContext) {
        // GIVEN
        val stepsExecuted = testContext.checkpoint(2)
        val processCompleted = testContext.checkpoint(2)
        val processDispatched = testContext.checkpoint()
        val repository = testRepository<String>(processCompleted)
        every { repository.saveProcess(any<FlowContext<Any>>()) } answers {
            testContext.failNow(RuntimeException("Save should never be called"))
            Future.succeededFuture()
        }
        val step: (data: String) -> Future<String> = {
            stepsExecuted.flag()
            Future.succeededFuture(it)
        }
        val startStep = Step.NoSave<String>(StepName("start"), StepName("end")) { step(it) }
        val workflow = testWorkflow(
            startNode = startStep.name,
            steps = listOf(startStep, Step.End(StepName("end")) { step(it) })
        )
        val engine = Engine(repository)
        val emptyData = ""

        // WHEN
        engine.start(workflow, emptyData, processId)
            .onSuccess { processDispatched.flag() }
            .onFailure { testContext.failNow(it) }
    }

    private fun <T> testRepository(processCompleted: Checkpoint): WorkflowEngineRepository {
        val repository = mockk<WorkflowEngineRepository>()
        every { repository.startNewProcess(processId) } returns Future.succeededFuture()
        val slot = slot<FlowContext<T>>()
        every { repository.getOrCreateProcess(flowContext = capture(slot)) } answers { Future.succeededFuture(slot.captured) }
        every { repository.saveProcess(any<FlowContext<T>>()) } answers { Future.succeededFuture() }
        every { repository.finishProcess(any<FlowContext<T>>()) } answers { processCompleted.flag(); Future.succeededFuture() }
        every { repository.removeProcessFromEngine(processId) } answers { processCompleted.flag(); Future.succeededFuture() }
        return repository
    }

    private fun singleStepWorkflow(step: (data: String) -> Future<String>): LongWorkflow<Any> {
        val singleStep = Step.End<String>(StepName("start")) { step(it) }
        return testWorkflow(
            startNode = singleStep.name,
            steps = listOf(singleStep)
        )
    }
}