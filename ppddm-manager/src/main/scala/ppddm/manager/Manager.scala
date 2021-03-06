package ppddm.manager

import com.typesafe.scalalogging.Logger
import ppddm.core.ai.DataMiningEngine
import ppddm.core.auth.AuthManager
import ppddm.core.util.URLUtil
import ppddm.core.db.{EmbeddedMongo, MongoDB}
import ppddm.manager.config.ManagerConfig
import ppddm.manager.gateway.ManagerHttpServer
import ppddm.manager.registry.AgentHealthChecker

/**
 * The starter object for PPDDM Manager
 */
object Manager {

  private val logger: Logger = Logger(this.getClass)

  var mongoDB: MongoDB = _
  var dataMiningEngine: DataMiningEngine = _

  def start(): Unit = {
    logger.info("PPDDM Manager is starting up...")

    if (ManagerConfig.mongoEmbedded) {
      // If it is configured to use an embedded Mongo instance
      logger.info("An embedded Mongo instance will be started.")
      EmbeddedMongo.start(ManagerConfig.appName, ManagerConfig.mongoHost, ManagerConfig.mongoPort)
    }

    mongoDB = MongoDB(
      ManagerConfig.appName,
      ManagerConfig.mongoHost,
      ManagerConfig.mongoPort,
      ManagerConfig.mongoDbName,
      ManagerConfig.mongodbUser,
      ManagerConfig.mongodbPassword,
      ManagerConfig.mongoAuthDbName,
      ManagerConfig.mongoPoolingMinSize,
      ManagerConfig.mongoPoolingMaxSize,
      ManagerConfig.mongoPoolingMaxWaitTime,
      ManagerConfig.mongoPoolingMaxConnectionLifeTime
    )
    if(ManagerConfig.mongoDbDrop) {
      // If it is configured to drop the database for a fresh start
     mongoDB.dropDatabase()
    }

    dataMiningEngine = DataMiningEngine(ManagerConfig.appName, ManagerConfig.sparkMaster)

    /* Import the ActorSystem */
    import ppddm.manager.config.ManagerExecutionContext._

    if(ManagerConfig.authEnabled) {
      AuthManager.init(
        URLUtil.append(ManagerConfig.authServerHost, ManagerConfig.loginPath),
        URLUtil.append(ManagerConfig.authServerHost, ManagerConfig.introspectionPath),
        ManagerConfig.authServerUsername,
        ManagerConfig.authServerPassword
      )
    }

    AgentHealthChecker.checkAllAgentsHealth()

    ManagerHttpServer.start(ManagerConfig.serverHost, ManagerConfig.serverPort, ManagerConfig.baseUri)

  }

}
