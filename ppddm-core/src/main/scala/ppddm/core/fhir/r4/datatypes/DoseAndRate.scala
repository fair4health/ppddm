package ppddm.core.fhir.r4.datatypes

class DoseAndRate(val id: Option[String], // coming from Element
                  val extension: Option[List[Extension]], // coming from Element
                  val `type`: Option[CodeableConcept],
                  val doseRange: Option[Range],
                  val doseQuantity: Option[Quantity],
                  val rateRatio: Option[Ratio],
                  val rateRange: Option[Range],
                  val rateQuantity: Option[Quantity])
