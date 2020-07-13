package ppddm.core.fhir.r4.datatypes

class Reference(val id: Option[String], // coming from Element
                val extension: Option[List[Extension]], // coming from Element
                val reference: Option[String],
                val `type`: Option[String], // type: uri
                val identifier: Option[Identifier],
                val display: Option[String]) {

}
