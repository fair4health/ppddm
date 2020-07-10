package ppddm.core.fhir.r4.datatypes

class CodeableConcept(val id: Option[String], // coming from Element
                      val extension: Option[List[Extension]], // coming from Element
                      val coding: Option[List[Coding]],
                      val text: Option[String]) {
}
