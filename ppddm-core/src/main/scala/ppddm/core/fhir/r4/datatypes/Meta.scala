package ppddm.core.fhir.r4.datatypes

import java.time.LocalDateTime

class Meta(val id: Option[String], // coming from Element
           val extension: Option[List[Extension]], // coming from Element
           val versionId: Option[String], // type: id
           val lastUpdated: Option[LocalDateTime], // type: instant
           val source: Option[String], // type: uri
           val profile: Option[List[String]], // type: canonical
           val security: Option[List[Coding]],
           val tag: Option[List[Coding]]) {
}
