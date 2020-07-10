package ppddm.core.fhir.r4.datatypes

import java.time.LocalDateTime

class Signature(val id: Option[String], // coming from Element
                val extension: Option[List[Extension]], // coming from Element
                val `type`: List[Coding],
                val when: LocalDateTime, // type: instant
                val who: Reference,
                val onBehalfOf: Option[Reference],
                val targetFormat: Option[String], // type: code
                val sigFormat: Option[String], // type: code
                val data: Option[String] // type: base64Binary
                ) {

}
