package ppddm.core.fhir.r4.datatypes

class Quantity(val id: Option[String], // coming from Element
               val extension: Option[List[Extension]], // coming from Element
               val value: Option[Double],
               val comparator: Option[String], // enum: QuantityComparator but TODO: how to do it for:  < | <= | >= | >
               val unit: Option[String],
               val system: Option[String], // type: uri
               val code: Option[String] // type: code
               ) {
}
