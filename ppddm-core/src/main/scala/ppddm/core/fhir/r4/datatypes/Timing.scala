package ppddm.core.fhir.r4.datatypes

import java.time.{LocalDateTime}

class Timing(val id: Option[String], // coming from Element
             val extension: Option[List[Extension]], // coming from Element
             val event: Option[List[LocalDateTime]],
             val repeat: Option[TimingRepeat],
             val code: Option[CodeableConcept]) {

}
