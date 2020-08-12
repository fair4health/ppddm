package ppddm.agent

import akka.actor.ActorSystem
import com.typesafe.scalalogging.Logger
import ppddm.agent.config.AgentConfig
import ppddm.agent.gateway.AgentHttpServer
import ppddm.core.ai.DataMiningEngine
import ppddm.core.fhir.r4.service.FHIRClient

/**
 * The starter object for a PPDDM Agent.
 */
object Agent {

  private val logger: Logger = Logger(this.getClass)

  var fhirClient: FHIRClient = _
  var dataMiningEngine: DataMiningEngine = _

  def start(): Unit = {
    logger.info("PPDDM Agent is starting up...")

    implicit val system: ActorSystem = ActorSystem("ppddm-agent")

    fhirClient = FHIRClient(AgentConfig.fhirHost, AgentConfig.fhirPort, AgentConfig.fhirPath, AgentConfig.fhirProtocol)
    dataMiningEngine = DataMiningEngine(AgentConfig.appName, AgentConfig.sparkMaster)

    AgentHttpServer.start(AgentConfig.serverHost, AgentConfig.serverPort, AgentConfig.baseUri)
  }

}
