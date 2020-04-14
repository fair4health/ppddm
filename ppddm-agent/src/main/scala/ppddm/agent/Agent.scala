package ppddm.agent

import akka.actor.ActorSystem
import com.typesafe.scalalogging.Logger
import ppddm.agent.config.AgentConfig
import ppddm.agent.gateway.AgentHttpServer
import ppddm.core.ai.DataMiningEngine

/**
 * The starter object for a PPDDM Agent.
 */
object Agent {

  private val logger: Logger = Logger(this.getClass)

  val dataMiningEngine: DataMiningEngine = DataMiningEngine(AgentConfig.appName, AgentConfig.sparkMaster)

  def start(): Unit = {
    logger.info("PPDDM Agent is starting up...")

    implicit val system: ActorSystem = ActorSystem("ppddm-agent")

    AgentHttpServer.start(AgentConfig.serverHost, AgentConfig.serverPort, AgentConfig.baseUri)
  }

}
