package ppddm.core.fhir.r4.service

case class FHIRClientException(clientCode: String = "", error: Option[String] = None, errorDesc: Option[String] = None) extends Exception {
  override def toString: String = {
    s"FHIR Client Exception: \nClient Code: $clientCode\nServer Code: ${error.getOrElse("Unknown")}\nServer Description: ${errorDesc.getOrElse("Unknown")}"
  }
}

