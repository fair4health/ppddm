package ppddm.core.util

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import ppddm.core.rest.model.ModelClass

import scala.language.implicitConversions

object JsonFormatter {

  implicit lazy val formats: Formats = JsonFormats.getFormats

  /**
   * Scala class that adds "parseJson" & "extract" methods to Strings
   */
  class JsonParsable(json: String) {
    def parseJson: JValue = {
      parse(json)
    }

    def extract[T: Manifest]: T = {
      parseJson.extract[T]
    }
  }

  /**
   * Scala class that adds "toJson" method for JObjects
   */
  class JsonConvertable(resource: ModelClass) {
    def toJson: String = {
      Serialization.write(resource)
    }

    def toPrettyJson: String = {
      Serialization.writePretty(resource)
    }
  }

  /**
   * Implicit conversion that ties the new JsonParsable class to the Scala Strings
   */
  implicit def parseFromJson(string: String): JsonParsable = new JsonParsable(string)

  /**
   * Implicit conversion that ties the new JsonConvertable class to Scala LinkedHashMaps
   */
  implicit def convertToJson(resource: ModelClass): JsonConvertable = new JsonConvertable(resource)
}

