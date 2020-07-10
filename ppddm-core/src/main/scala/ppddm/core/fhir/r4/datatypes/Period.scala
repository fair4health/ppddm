package ppddm.core.fhir.r4.datatypes

import java.time.LocalDateTime

class Period(val id: Option[String], // coming from Element
             val extension: Option[List[Extension]], // coming from Element
             val start: Option[LocalDateTime],
             val end: Option[LocalDateTime]) {

}
