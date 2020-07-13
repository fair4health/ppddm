package ppddm.core.fhir.r4.resources

import java.time.LocalDateTime

import ppddm.core.fhir.r4.datatypes.{Annotation, CodeableConcept, Dosage, Extension, Identifier, Meta, Narrative, Period, Reference}

object MedicationStatementStatus extends Enumeration {
  type MedicationStatementStatus = Value
  val active, completed, `entered-in-error`, intended,
  stopped, `on-hold`, unknown, `not-taken` = Value
}

class MedicationStatement(override val id: Option[String], // coming from Resource
                          override val meta: Option[Meta], // coming from Resource
                          override val implicitRules: Option[String], // type: uri | coming from Resource
                          override val language: Option[String], // type: code | coming from Resource
                          val text: Option[Narrative], // coming from DomainResource
                          val contained: Option[List[Resource]], // coming from DomainResource
                          val extension: Option[List[Extension]], // coming from DomainResource
                          val identifier: Option[List[Identifier]],
                          val basedOn: Option[List[Reference]],
                          val partOf: Option[List[Reference]],
                          val status: String, // enum: MedicationStatementStatus
                          val statusReason: Option[List[CodeableConcept]],
                          val category: Option[CodeableConcept],
                          val medicationCodeableConcept: CodeableConcept,
                          val medicationReference: Reference,
                          val subject: Reference,
                          val context: Option[Reference],
                          val effectiveDateTime: Option[LocalDateTime],
                          val effectivePeriod: Option[Period],
                          val dateAsserted: Option[LocalDateTime],
                          val informationSource: Option[Reference],
                          val derivedFrom: Option[List[Reference]],
                          val reasonCode: Option[List[CodeableConcept]],
                          val reasonReference: Option[List[Reference]],
                          val note: Option[List[Annotation]],
                          val dosage: Option[List[Dosage]]
                         ) extends Resource("MedicationStatement", id, meta, implicitRules, language) {

}
