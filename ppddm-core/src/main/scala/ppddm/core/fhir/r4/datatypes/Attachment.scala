package ppddm.core.fhir.r4.datatypes

import java.time.LocalDateTime

import com.google.common.primitives.UnsignedInteger

class Attachment(val id: Option[String], // coming from Element
                 val extension: Option[List[Extension]], // coming from Element
                 val contentType: Option[String],
                 val language: Option[String],
                 val data: Option[String], // type: base64Binary
                 val url: Option[String], // type: url
                 val size: Option[UnsignedInteger],
                 val hash: Option[String], // type: base64Binary
                 val title: Option[String],
                 val creation: Option[LocalDateTime]) {

}
