package ppddm.core.fhir.r4.resources

import java.time.{LocalDateTime, LocalTime}

import ppddm.core.fhir.r4.datatypes.{Annotation, CodeableConcept, Extension, Identifier, Meta, Narrative, Period, Quantity, Ratio, Reference, ReferenceRange, SampledData, Timing}

object ObservationStatus extends Enumeration {
  type ObservationStatus = Value
  val registered, preliminary, `final`, amended,
  corrected, cancelled, `entered-in-error`, unknown = Value
}

class ObservationComponent(val code: CodeableConcept,
                           val valueQuantity: Option[Quantity],
                           val valueCodeableConcept: Option[CodeableConcept],
                           val valueString: Option[String],
                           val valueBoolean: Option[Boolean],
                           val valueInteger: Option[Int],
                           val valueRange: Option[Range],
                           val valueRatio: Option[Ratio],
                           val valueSampledData: Option[SampledData],
                           val valueTime: Option[LocalTime],
                           val valueDateTime: Option[LocalDateTime],
                           val valuePeriod: Option[Period],
                           val dataAbsentReason: Option[CodeableConcept],
                           val interpretation: Option[List[CodeableConcept]],
                           val referenceRange: Option[List[ReferenceRange]])

class Observation(override val id: Option[String],
                  override val meta: Option[Meta],
                  override val implicitRules: Option[String], // type: uri
                  override val language: Option[String], // type: code
                  val text: Option[Narrative], // coming from DomainResource
                  val contained: Option[List[Resource]], // coming from DomainResource
                  val extension: Option[List[Extension]], // coming from DomainResource
                  val identifier: Option[List[Identifier]],
                  val basedOn: Option[List[Reference]],
                  val partOf: Option[List[Reference]],
                  val status: String, // enum: ObservationStatus
                  val category: Option[List[CodeableConcept]],
                  val code: CodeableConcept,
                  val subject: Option[Reference],
                  val encounter: Option[Reference],
                  val effectiveDateTime: Option[LocalDateTime],
                  val effectivePeriod: Option[Period],
                  val effectiveTiming: Option[Timing],
                  val effectiveInstant: Option[LocalDateTime], // type: instant
                  val issued: Option[LocalDateTime], // type: instant
                  val performer: Option[List[Reference]],
                  val valueQuantity: Option[Quantity],
                  val valueCodeableConcept: Option[CodeableConcept],
                  val valueString: Option[String],
                  val valueBoolean: Option[Boolean],
                  val valueInteger: Option[Int],
                  val valueRange: Option[Range],
                  val valueRatio: Option[Ratio],
                  val valueSampledData: Option[SampledData],
                  val valueTime: Option[LocalTime],
                  val valueDateTime: Option[LocalDateTime],
                  val valuePeriod: Option[Period],
                  val dataAbsentReason: Option[CodeableConcept],
                  val interpretation: Option[List[CodeableConcept]],
                  val note: Option[List[Annotation]],
                  val bodySite: Option[CodeableConcept],
                  val method: Option[CodeableConcept],
                  val specimen: Option[Reference],
                  val device: Option[Reference],
                  val referenceRange: Option[ReferenceRange],
                  val hasMember: Option[List[Reference]],
                  val derivedFrom: Option[List[Reference]],
                  val component: Option[List[ObservationComponent]]
                 ) extends Resource("Observation", id, meta, implicitRules, language) {

}
