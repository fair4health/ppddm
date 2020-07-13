package ppddm.core.fhir.r4.datatypes

class ReferenceRange(val id: Option[String], // coming from Element
                     val extension: Option[List[Extension]], // coming from Element
                     val low: Option[Quantity],
                     val high: Option[Quantity],
                     val `type`: Option[CodeableConcept],
                     val appliesTo: Option[List[CodeableConcept]],
                     val age: Option[Range],
                     val text: Option[String]) {

}
