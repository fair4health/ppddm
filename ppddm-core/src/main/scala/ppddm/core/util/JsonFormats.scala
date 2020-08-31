package ppddm.core.util

import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

object JsonFormats {
  def getFormats: Formats = Serialization.formats(NoTypeHints) + JavaDateTimeSerializers.LocalDateTimeSerializer
}
