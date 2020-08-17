package ppddm.agent

import akka.actor.ActorSystem
import ca.uhn.fhir.context.FhirVersionEnum
import com.typesafe.scalalogging.Logger
import ppddm.agent.config.AgentConfig
import ppddm.agent.gateway.AgentHttpServer
import ppddm.core.ai.DataMiningEngine
import ppddm.core.fhir.FhirRestSource
import ppddm.core.fhir.r4.service.FHIRClient

/**
 * The starter object for a PPDDM Agent.
 */
object Agent {

  private val logger: Logger = Logger(this.getClass)

  var fhirClient: FHIRClient = _ // TODO to be deleted
  var fhirRestSource: FhirRestSource = _
  var dataMiningEngine: DataMiningEngine = _

  def start(): Unit = {
    logger.info("PPDDM Agent is starting up...")

    implicit val system: ActorSystem = ActorSystem("ppddm-agent")

    fhirClient = FHIRClient(AgentConfig.fhirHost, AgentConfig.fhirPort, AgentConfig.fhirPath, AgentConfig.fhirProtocol) // TODO to be deleted
    fhirRestSource = new FhirRestSource(AgentConfig.fhirHost, AgentConfig.fhirPort, AgentConfig.fhirPath, AgentConfig.fhirProtocol, FhirVersionEnum.R4, 100)
    dataMiningEngine = DataMiningEngine(AgentConfig.appName, AgentConfig.sparkMaster)

    AgentHttpServer.start(AgentConfig.serverHost, AgentConfig.serverPort, AgentConfig.baseUri)
  }

}
