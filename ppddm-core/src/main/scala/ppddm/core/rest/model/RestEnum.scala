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
