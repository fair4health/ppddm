package ppddm.core.util

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.json4s.CustomSerializer
import org.json4s.JsonAST.{JNull, JString}

object JavaDateTimeSerializers {

  val dateTimeFormatter: DateTimeFormatter =  DateTimeFormatter.ofPattern("yyyy[-MM[-dd['T'HH[:mm[:ss[.SSS][XXX]]]]]]")

  case object LocalDateTimeSerializer extends CustomSerializer[LocalDateTime](format => (
    {
      case JString(s) => LocalDateTime.parse(s, dateTimeFormatter)
      case JNull => null
    },
    {
      case d: LocalDateTime => JString(d.format(dateTimeFormatter))
    }
  ))

}
