package ppddm.core.fhir.r4.datatypes

class Coding(val id: Option[String], // coming from Element
             val extension: Option[List[Extension]], // coming from Element
             val system: Option[String], // type: uri
             val version: Option[String],
             val code: Option[String], // type: code
             val display: Option[String],
             val userSelected: Option[Boolean]) {
}
