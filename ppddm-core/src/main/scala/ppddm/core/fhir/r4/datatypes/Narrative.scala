package ppddm.core.fhir.r4.datatypes

object NarrativeStatus extends Enumeration {
  type NarrativeStatus = Value
  val generated, extensions, additional, empty = Value
}

class Narrative(val id: Option[String], // coming from Element
                val extension: Option[List[Extension]], // coming from Element
                val status: String, // enum: NarrativeStatus
                val div: String, // type: xhtml
                ) {
}
