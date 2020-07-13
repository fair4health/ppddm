package ppddm.core.fhir.r4.datatypes

object AddressUse extends Enumeration {
  type AddressUse = Value
  val home, work, temp, old, billing = Value
}

object AddressType extends Enumeration {
  type AddressType = Value
  val postal, physical, both = Value
}

class Address(val id: Option[String], // coming from Element
              val extension: Option[List[Extension]], // coming from Element
              val use: Option[String], // enum: AddressUse
              val `type`: Option[String], // enum: AddressType
              val text: Option[String],
              val line: Option[List[String]],
              val city: Option[String],
              val district: Option[String],
              val state: Option[String],
              val postalCode: Option[String],
              val country: Option[String],
              val period: Option[Period]) {

}
