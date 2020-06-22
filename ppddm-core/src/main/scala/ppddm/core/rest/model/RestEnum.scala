package ppddm.core.rest.model

object ProjectType extends Enumeration {
  type ProjectType = String
  val PREDICTION = "prediction"
  val ASSOCIATION = "association"
}
