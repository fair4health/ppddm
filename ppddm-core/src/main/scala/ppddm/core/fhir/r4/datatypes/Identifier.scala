package ppddm.core.fhir.r4.datatypes

object IdentifierUse extends Enumeration {
  type IdentifierUse = Value
  val usual, official, temp, secondary, old = Value
}

class Identifier(val id: Option[String], // coming from Element
                 val extension: Option[List[Extension]], // coming from Element
                 val use: Option[String], // enum: IdentifierUse
                 val `type`: Option[CodeableConcept],
                 val system: Option[String], // type: uri
                 val value: Option[String],
                 val period: Option[Period],
                 val assigner: Option[Reference]) {
}
