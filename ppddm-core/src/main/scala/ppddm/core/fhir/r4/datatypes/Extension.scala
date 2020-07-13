package ppddm.core.fhir.r4.datatypes

import java.time.{LocalDateTime, LocalTime}

class Extension(val url: String, // type: uri
                val extension: Option[List[Extension]],
                val valueBase64Binary: Option[String],
                val valueBoolean: Option[Boolean],
                val valueCode: Option[String],
                val valueDate: Option[LocalDateTime],
                val valueDateTime: Option[LocalDateTime],
                val valueDecimal: Option[Double],
                val valueId: Option[String], // type: id
                val valueInstant: Option[LocalDateTime], // type: instant
                val valueInteger: Option[Int],
                val valueMarkdown: Option[String], // type: markdown
                val valueOid: Option[String], // type: oid
                val valuePositiveInt: Option[Int], // type: positiveInt
                val valueString: Option[String],
                val valueTime: Option[LocalTime],
                val valueUnsignedInt: Option[Int], // type: unsignedInt
                val valueUri: Option[String], // type: uri
                val valueAddress: Option[Address],
                val valueAge: Option[Quantity],
                val valueAnnotation: Option[Annotation],
                val valueAttachment: Option[Attachment],
                val valueCodeableConcept: Option[CodeableConcept],
                val valueCoding: Option[Coding],
                val valueContactPoint: Option[ContactPoint],
                val valueCount: Option[Quantity],
                val valueDistance: Option[Quantity],
                val valueDuration: Option[Quantity],
                val valueHumanName: Option[HumanName],
                val valueIdentifier: Option[Identifier],
                val valueMoney: Option[Money],
                val valuePeriod: Option[Period],
                val valueQuantity: Option[Quantity],
                val valueRange: Option[Range],
                val valueRatio: Option[Ratio],
                val valueReference: Option[Reference],
                val valueSampledData: Option[SampledData],
                val valueSignature: Option[Signature],
                val valueTiming: Option[Timing],
                val valueMeta: Option[Meta]) {
}
