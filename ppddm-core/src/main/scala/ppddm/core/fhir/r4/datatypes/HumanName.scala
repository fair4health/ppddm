package ppddm.core.fhir.r4.datatypes

object HumanNameUse extends Enumeration {
  type HumanNameUse = Value
  val usual, official, temp, nickname, anonymous, old, maiden = Value
}

class HumanName(val id: Option[String], // coming from Element
                val extension: Option[List[Extension]], // coming from Element
                val use: Option[String], // enum: HumanNameUse
                val text: Option[String],
                val family: Option[String],
                val given: Option[List[String]],
                val prefix: Option[List[String]],
                val suffix: Option[List[String]],
                val period: Option[Period]) {

}
