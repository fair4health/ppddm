package ppddm.core.fhir.r4.datatypes

import java.time.LocalDateTime

class Annotation(val id: Option[String], // coming from Element
                 val extension: Option[List[Extension]], // coming from Element
                 val authorReference: Option[Reference],
                 val authorString: Option[String],
                 val time: Option[LocalDateTime],
                 val text: String) {

}
