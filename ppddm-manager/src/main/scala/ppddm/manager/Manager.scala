package ppddm.manager

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import com.typesafe.scalalogging.Logger
import org.mongodb.scala.bson.collection.immutable.Document
import ppddm.core.ai.DataMiningEngine
import ppddm.core.db.{EmbeddedMongo, MongoDB}
import ppddm.manager.config.ManagerConfig
import ppddm.manager.gateway.ManagerHttpServer

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * The starter object for PPDDM Manager
 */
object Manager {

  private val logger: Logger = Logger(this.getClass)

  var mongoDB: MongoDB = _
  var dataMiningEngine: DataMiningEngine = _

  def start(): Unit = {
    logger.info("PPDDM Manager is starting up...")

    implicit val system: ActorSystem = ActorSystem("ppddm-manager")

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

    ManagerHttpServer.start(ManagerConfig.serverHost, ManagerConfig.serverPort, ManagerConfig.baseUri)

    val doc: Document = Document("name" -> "MongoDB", "type" -> "database",
      "count" -> 1, "info" -> Document("x" -> 203, "y" -> 102))
    import scala.concurrent.ExecutionContext.Implicits.global
    val f = mongoDB.getCollection("test").insertOne(doc).head().map(r => println("ID:L" + r.getInsertedId))
    Await.result(f, Duration(10, TimeUnit.SECONDS))

  }

}
