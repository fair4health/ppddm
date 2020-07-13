package ppddm.core.fhir.r4.datatypes

class SampledData(val id: Option[String], // coming from Element
                  val extension: Option[List[Extension]], // coming from Element
                  val origin: Quantity,
                  val period: Double,
                  val factor: Option[Double],
                  val lowerLimit: Option[Double],
                  val upperLimit: Option[Double],
                  val dimensions: Option[Int], // type: positiveInt
                  val data: Option[String]) {

}
