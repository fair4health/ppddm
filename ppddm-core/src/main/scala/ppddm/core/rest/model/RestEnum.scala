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

object SelectionStatus extends Enumeration {
  type SelectionStatus = String
  val SELECTED = "selected"
  val DISCARDED = "discarded"
}

/**
 * ExecutionState is being used by Dataset, DatasetSource, DataMiningModel and Algorithm classes of the rest model.
 *
 * Dataset: When a Dataset is created, it will invoke Agents to prepare their data. The ExecutionState of the Dataset
 * will be EXECUTING at this point. Once all Agents prepare their data, th ExecutionState of the Dataset will be
 * READY since the user now will select/discard the data sources of the Dataset. After this selection,
 * the ExecutionState will be FINAL.
 *
 * DatasetSource: When an Agent is invoked to prepare its data, the associated DatasetSource's ExecutionState will be
 * EXECUTING. When the data is ready at the Agent, then the ExecutionState will be FINAL. Hence, DatasetSource uses only
 * two of the available ExecutionState enumeration values.
 *
 * DataMiningModel:
 *
 */
object ExecutionState extends Enumeration {
  type ExecutionState = String
  val EXECUTING = "executing"
  val READY = "ready"
  val FINAL = "final"
}

object FHIRPathExpressionPrefix extends Enumeration {
  type FHIRPathExpressionPrefix = String
  val AGGREGATION = "aggr:"
  val VALUE = "value:"
  val SATISFY = "satisfy:"
}

object DataType extends Enumeration {
  type DataType = String
  val INTEGER = "integer"
  val DOUBLE = "double"
  val STRING = "string"
}
