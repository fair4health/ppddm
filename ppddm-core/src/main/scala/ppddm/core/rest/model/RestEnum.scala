package ppddm.core.rest.model

object ProjectType extends Enumeration {
  type ProjectType = String
  val PREDICTION = "prediction"
  val ASSOCIATION = "association"
}

object VariableDataType extends Enumeration {
  type VariableDataType = String
  val CATEGORICAL = "categorical"
  val NUMERIC = "numeric"
}

object VariableType extends Enumeration {
  type VariableType = String
  val INDEPENDENT = "independent"
  val DEPENDENT = "dependent"
}

object DataSourceSelectionStatus extends Enumeration {
  type DataSourceSelectionStatus = String
  val SELECTED = "selected"
  val DISCARDED = "discarded"
}

/**
 * ExecutionState is being used by Dataset and DatasetSource classes of the model.
 *
 * Dataset: When a Dataset is created, it will invoke Agents to prepare their data. The ExecutionState of the Dataset
 * will be QUERYING at this point. Once all Agents prepare their data, th ExecutionState of the Dataset will be
 * IN_PROGRESS since the user now will select/discard the Dataset. After this selection, the ExecutionState will be FINAL.
 *
 * DatasetSource: When an Agent is invoked to prepare its data, the associated DatasetSource's ExecutionState will be
 * QUERYING. When the data is ready at the Agent, then the ExecutionState will be FINAL. Hence, DatasetSource uses only
 * two of the available ExecutionState enumeration values.
 *
 */
object ExecutionState extends Enumeration {
  type ExecutionState = String
  val QUERYING = "querying"
  val IN_PROGRESS = "in_progress"
  val FINAL = "final"
}
