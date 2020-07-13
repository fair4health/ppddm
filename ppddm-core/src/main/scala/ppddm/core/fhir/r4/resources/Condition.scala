package ppddm.core.fhir.r4.resources

import java.time.LocalDateTime

import ppddm.core.fhir.r4.datatypes.{Annotation, CodeableConcept, Extension, Identifier, Meta, Narrative, Period, Quantity, Reference}

object ConditionClinicalStatus extends Enumeration {
  type ConditionClinicalStatus = Value
  val active, recurrence, relapse, inactive, remission, resolved = Value
}

object ConditionVerificationStatus extends Enumeration {
  type ConditionVerificationStatus = Value
  val unconfirmed, provisional, differential, confirmed, refuted, `entered-in-error` = Value
}

object ConditionCategory extends Enumeration {
  type ConditionCategory = Value
  val `problem-list-item`, `encounter-diagnosis` = Value
}

class ConditionStage(val summary: Option[CodeableConcept],
                     val assessment: Option[List[Reference]],
                     val `type`: Option[CodeableConcept])

class ConditionEvidence(val code: Option[List[CodeableConcept]],
                        val detail: Option[List[Reference]])

class Condition(override val id: Option[String],
                override val meta: Option[Meta],
                override val implicitRules: Option[String], // type: uri
                override val language: Option[String], // type: code
                val text: Option[Narrative], // coming from DomainResource
                val contained: Option[List[Resource]], // coming from DomainResource
                val extension: Option[List[Extension]], // coming from DomainResource
                val identifier: Option[List[Identifier]],
                val clinicalStatus: Option[CodeableConcept], // enum: ConditionClinicalStatus
                val verificationStatus: Option[CodeableConcept], // enum: ConditionVerificationStatus
                val category: Option[List[CodeableConcept]], // enum: ConditionCategory
                val severity: Option[CodeableConcept],
                val code: Option[CodeableConcept],
                val bodySite: Option[List[CodeableConcept]],
                val subject: Reference,
                val encounter: Option[Reference],
                val onsetDateTime: Option[LocalDateTime],
                val onsetAge: Option[Quantity],
                val onsetPeriod: Option[Period],
                val onsetRange: Option[Range],
                val onsetString: Option[String],
                val abatementDateTime: Option[LocalDateTime],
                val abatementAge: Option[Quantity],
                val abatementPeriod: Option[Period],
                val abatementRange: Option[Range],
                val abatementString: Option[String],
                val recordedDate: Option[LocalDateTime],
                val recorder: Option[Reference],
                val asserter: Option[Reference],
                val stage: Option[ConditionStage],
                val evidence: Option[List[ConditionEvidence]],
                val note: Option[List[Annotation]]
               ) extends Resource("Condition", id, meta, implicitRules, language) {

}
