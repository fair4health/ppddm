package ppddm.core.fhir.r4.resources

import ppddm.core.fhir.r4.datatypes.Meta

import scala.reflect.ClassTag

class Resource(val resourceType: String,
               val id: Option[String],
               val meta: Option[Meta],
               val implicitRules: Option[String], // type: uri
               val language: Option[String], // type: code
               ) {
}
