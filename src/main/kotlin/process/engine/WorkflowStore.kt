package process.engine

import java.io.Serializable

data class WorkflowStore(val workflows: Map<String, Workflow<Any>>) : Serializable