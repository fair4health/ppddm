package ppddm.core.fhir.r4.datatypes

class Money(val id: Option[String], // coming from Element
            val extension: Option[List[Extension]], // coming from Element
            val value: Option[Double],
            val currency: Option[String] // type: code
           ) {

}
