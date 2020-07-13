package ppddm.core.fhir.r4.datatypes

class Range(val id: Option[String], // coming from Element
            val extension: Option[List[Extension]], // coming from Element
            val low: Option[Quantity],
            val high: Option[Quantity]) {

}
