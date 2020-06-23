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

object DatasetStatus extends Enumeration {
  type DatasetStatus = String
  val QUERYING = "querying"
  val QUERIED_ALL = "queried_all"
  val FINAL = "final"
}

object DataSourceStatus extends Enumeration {
  type DataSourceStatus = String
  val QUERYING = "querying"
  val STATISTICS_READY = "statistics_ready"
  val DISCARDED = "discarded"
  val SELECTED = "selected"
}
