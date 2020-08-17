package ppddm.core.fhir

import ca.uhn.fhir.context.{FhirContext, FhirVersionEnum}
import ca.uhn.fhir.rest.client.api.{IGenericClient, IRestfulClientFactory, ServerValidationModeEnum}
import org.apache.log4j.LogManager

class FHIRClient(host: String,
                 port: Int,
                 path: String,
                 protocol: String,
                 batchSize: Int) extends Serializable {

  @transient lazy val logger = LogManager.getLogger("transformerLogger")

  @transient
  var fhirContext: FhirContext = FhirContext.forR4()

  /**
   * Client factory to create HAPI Fhir client
   */
  @transient
  lazy val clientFactory: IRestfulClientFactory = getClientFactory()

  // onFHIR.io server path
  private val fhirServerBaseURI = if (path.startsWith("/")) s"$protocol://$host:$port$path" else s"$protocol://$host:$port/$path"

  /**
   * Generate the HAPI Client factory
   *
   * @return
   */
  private def getClientFactory(): IRestfulClientFactory = {
    val factory = fhirContext.getRestfulClientFactory
    factory.setServerValidationMode(ServerValidationModeEnum.NEVER)
    factory.setConnectTimeout(300000)
    factory.setSocketTimeout(300000)
    factory
  }

  /**
   * Generate the HAPI Client
   *
   * @return
   */
  def getClient(): IGenericClient = {
    clientFactory.newGenericClient(fhirServerBaseURI)
  }

}

object FHIRClient {
  def apply(host: String,
            port: Int,
            path: String,
            protocol: String = "http",
            batchSize: Int = 100): FHIRClient = {
    new FHIRClient(host, port, path, protocol, batchSize)
  }
}
