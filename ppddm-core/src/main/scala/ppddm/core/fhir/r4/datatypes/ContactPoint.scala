package ppddm.core.fhir.r4.datatypes

object ContactPointSystem extends Enumeration {
  type ContactPointSystem = Value
  val phone, fax, email, pager, url, sms, other = Value
}

object ContactPointUse extends Enumeration {
  type ContactPointUse = Value
  val home, work, temp, old, mobile = Value
}

class ContactPoint(val id: Option[String], // coming from Element
                   val extension: Option[List[Extension]], // coming from Element
                   val system: Option[String], // enum: ContactPointSystem
                   val value: Option[String],
                   val use: Option[String], // enum: ContactPointUse
                   val rank: Option[String], // type: positiveInt
                   val period: Option[Period]) {

}
