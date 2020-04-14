package ppddm.manager

import akka.actor.ActorSystem
import com.typesafe.scalalogging.Logger
import ppddm.core.ai.DataMiningEngine
import ppddm.manager.config.ManagerConfig
import ppddm.manager.gateway.ManagerHttpServer

/**
 * The starter object for PPDDM Manager
 */
object Manager {

  private val logger: Logger = Logger(this.getClass)

  val dataMiningEngine: DataMiningEngine = DataMiningEngine(ManagerConfig.appName, ManagerConfig.sparkMaster)

  def start(): Unit = {
    logger.info("PPDDM Manager is starting up...")

    implicit val system: ActorSystem = ActorSystem("ppddm-manager")

    ManagerHttpServer.start(ManagerConfig.serverHost, ManagerConfig.serverPort, ManagerConfig.baseUri)
  }

}
