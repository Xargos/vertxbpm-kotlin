package process.engine

import java.io.Serializable

data class WorkflowStore(
    val instantWorkflows: Map<String, () -> InstantWorkflow<Any, Any>> = mapOf(),
    val longWorkflows: Map<String, () -> LongWorkflow<Any>> = mapOf()
) : Serializable