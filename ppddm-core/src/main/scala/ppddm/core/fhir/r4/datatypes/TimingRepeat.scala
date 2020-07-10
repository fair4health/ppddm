package ppddm.core.fhir.r4.datatypes

import java.time.LocalTime

import com.google.common.primitives.UnsignedInteger

object UnitsOfTime extends Enumeration {
  type UnitsOfTime = Value
  val s, min, h, d, wk, mo, a = Value
}

object DaysOfWeek extends Enumeration {
  type DaysOfWeek = Value
  val mon, tue, wed, thu, fri, sat, sun = Value
}

class TimingRepeat(val id: Option[String], // coming from Element
                   val extension: Option[List[Extension]], // coming from Element
                   val boundsDuration: Option[Quantity],
                   val boundsRange: Option[Range],
                   val boundsPeriod: Option[Period],
                   val count: Option[Int], // type: positiveInt
                   val countMax: Option[Int], // type: positiveInt
                   val duration: Option[Double],
                   val durationMax: Option[Double],
                   val durationUnit: Option[String], // enum: UnitsOfTime
                   val frequency: Option[Int], // type: positiveInt
                   val frequencyMax: Option[Int], // type: positiveInt
                   val period: Option[Double],
                   val periodMax: Option[Double],
                   val periodUnit: Option[String], // enum: UnitsOfTime
                   val dayOfWeek: Option[List[String]], // enum: DaysOfWeek
                   val timeOfDay: Option[List[LocalTime]],
                   val when: Option[List[String]], // type: code
                   val offset: Option[UnsignedInteger]) {

}
