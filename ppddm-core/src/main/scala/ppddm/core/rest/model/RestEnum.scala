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

object ExecutionState extends Enumeration {
  type ExecutionState = String
  val QUERYING = "querying"
  val IN_PROGRESS = "in_progress"
  val FINAL = "final"
}

object FHIRPathExpressionPrefix extends Enumeration {
  type FHIRPathExpressionPrefix = String
  val AGGREGATION = "aggr:"
  val VALUE = "value:"
  val SATISFY = "satisfy:"
}
