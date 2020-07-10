package ppddm.core.fhir.r4.resources

import java.time.LocalDateTime

import ppddm.core.fhir.r4.datatypes.{CodeableConcept, Extension, Identifier, Meta, Narrative, Ratio, Reference}

object MedicationStatus extends Enumeration {
  type MedicationStatus = Value
  val active, inactive, `entered-in-error` = Value
}

class MedicationIngredient(val itemCodeableConcept: CodeableConcept,
                           val itemReference: Reference,
                           val isActive: Option[Boolean],
                           val amount: Option[Ratio])

class MedicationBatch(val lotNumber: Option[String],
                   val expirationDate: Option[LocalDateTime])

class Medication(override val id: Option[String],
                 override val meta: Option[Meta],
                 override val implicitRules: Option[String], // type: uri
                 override val language: Option[String], // type: code
                 val text: Option[Narrative], // coming from DomainResource
                 val contained: Option[List[Resource]], // coming from DomainResource
                 val extension: Option[List[Extension]], // coming from DomainResource
                 val identifier: Option[List[Identifier]],
                 val code: Option[CodeableConcept],
                 val status: Option[String], // enum: MedicationStatus
                 val manufacturer: Option[Reference],
                 val form: Option[CodeableConcept],
                 val ingredient: Option[MedicationIngredient],
                 val batch: Option[MedicationBatch]
                ) extends Resource("Medication", id, meta, implicitRules, language) {

}
