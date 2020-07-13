package ppddm.core.fhir.r4.datatypes

class Ratio(val id: Option[String], // coming from Element
            val extension: Option[List[Extension]], // coming from Element
            val numerator: Option[Quantity],
            val denominator: Option[Quantity]) {

}
