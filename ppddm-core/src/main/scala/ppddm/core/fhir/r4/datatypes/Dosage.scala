package ppddm.core.fhir.r4.datatypes

class Dosage(val id: Option[String], // coming from Element
             val extension: Option[List[Extension]], // coming from Element
             val sequence: Option[Int],
             val text: Option[String],
             val additionalInstruction: Option[List[CodeableConcept]],
             val patientInstruction: Option[String],
             val timing: Option[Timing],
             val asNeededBoolean: Option[Boolean],
             val asNeededCodeableConcept: Option[CodeableConcept],
             val site: Option[CodeableConcept],
             val route: Option[CodeableConcept],
             val method: Option[CodeableConcept],
             val doseAndRate: Option[List[DoseAndRate]],
             val maxDosePerPeriod: Option[Ratio],
             val maxDosePerAdministration: Option[Quantity],
             val maxDosePerLifetime: Option[Quantity]) {

}
