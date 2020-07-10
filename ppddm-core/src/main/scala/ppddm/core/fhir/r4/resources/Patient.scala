package ppddm.core.fhir.r4.resources

import java.time.{LocalDate, LocalDateTime}

import ppddm.core.fhir.r4.datatypes.{Address, Attachment, CodeableConcept, ContactPoint, Extension, HumanName, Identifier, Meta, Narrative, Period, Reference}

object AdministrativeGender extends Enumeration {
  type AdministrativeGender = Value
  val male, female, other, unknown = Value
}

object LinkType extends Enumeration {
  type LinkType = Value
  val `replaced-by`, replaces, refer, seealso = Value
}

class PatientContact(val relationship: Option[List[CodeableConcept]],
                     val name: Option[HumanName],
                     val telecom: Option[List[ContactPoint]],
                     val address: Option[Address],
                     val gender: Option[String], // enum: AdministrativeGender
                     val organization: Option[Reference],
                     val period: Option[Period]) {

}

class PatientCommunication(val language: CodeableConcept,
                           val preferred: Option[Boolean]) {

}

class PatientLink(val other: Reference,
                  val `type`: String // enum: LinkType
                  ) {
}

class Patient(override val id: Option[String],
              override val meta: Option[Meta],
              override val implicitRules: Option[String], // type: uri
              override val language: Option[String], // type: code
              val text: Option[Narrative], // coming from DomainResource
              val contained: Option[List[Resource]], // coming from DomainResource
              val extension: Option[List[Extension]], // coming from DomainResource
              val identifier: Option[List[Identifier]],
              val active: Option[Boolean],
              val name: Option[List[HumanName]],
              val telecom: Option[List[ContactPoint]],
              val gender: Option[String], // enum: AdministrativeGender
              val birthDate: Option[LocalDate],
              val deceasedBoolean: Option[Boolean],
              val deceasedDateTime: Option[LocalDateTime],
              val address: Option[List[Address]],
              val maritalStatus: Option[CodeableConcept],
              val multipleBirthBoolean: Option[Boolean],
              val multipleBirthInteger: Option[Int],
              val photo: Option[List[Attachment]],
              val contact: Option[List[PatientContact]],
              val communication: Option[List[PatientCommunication]],
              val generalPractitioner: Option[List[Reference]],
              val managingOrganization: Option[Reference],
              val link: Option[List[PatientLink]]
             ) extends Resource("Patient", id, meta, implicitRules, language) {

}
