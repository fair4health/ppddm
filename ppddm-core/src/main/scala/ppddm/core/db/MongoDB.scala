package ppddm.core.db

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.Logger
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.connection.ConnectionPoolSettings
import org.mongodb.scala.{MongoClient, MongoClientSettings, MongoCredential, MongoDatabase, ServerAddress}
import ppddm.core.rest.model.ModelClass

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * MongoDB client
 *
 * @param mongoClientSettings Settings object specific to the MongoDB to be connected
 * @param dbName              Name of the database to be connected
 */
class MongoDB(mongoClientSettings: MongoClientSettings, dbName: String) {

  import MongoDB.logger

  /**
   * The Codec registry so that case classes inheriting from #ModelClass can be converted to BSON automatically
   */
  private val codecRegistry = fromRegistries(fromProviders(classOf[ModelClass]), DEFAULT_CODEC_REGISTRY)

  private val mongoClient: MongoClient = MongoClient(mongoClientSettings)
  private var mongoDatabase: MongoDatabase = _getDatabase()

  private def _getDatabase(): MongoDatabase = {
    mongoClient.getDatabase(dbName).withCodecRegistry(codecRegistry)
  }

  /**
   * Get the Mongo database
   *
   * @return A MongoDatabase
   */
  def getDatabase: MongoDatabase = {
    mongoDatabase
  }

  /**
   * Drop the Mongo database (possibly for a fresh start)
   */
  def dropDatabase(): Unit = {
    val f = getDatabase.drop().toFuture()
    try {
      Await.result(f, Duration(10, TimeUnit.SECONDS))
    } catch {
      case e: java.util.concurrent.TimeoutException =>
        logger.error("Dropping Mongo database:{} has not finished within 10 seconds", dbName, e)
    } finally {
      logger.info("Mongo database:{} is dropped.", dbName)
    }
    mongoDatabase = _getDatabase() // After dropping it, get the database again from the connection
  }

}

/**
 * Companion object for the MongoDB class
 */
object MongoDB {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Class constructor for a MongoDB class through this companion object
   *
   * @param appName                   Name of the application using this MongoDB client
   * @param host                      Hostname of the MongoDB to be connected
   * @param port                      Port number of the MongoDB to be connected
   * @param dbName                    Name of the database to tbe connected
   * @param user                      username for authentication. If provided together with password and authDbName, a secure connection will be established
   * @param password                  password for authentication. If provided together with user and authDbName, a secure connection will be established
   * @param authDbName                database name to be used for authentication. If provided together with user and password, a secure connection will be established
   * @param poolMinSize               minimum number of the connections in the connection pool. If provided, a connection pool is created
   * @param poolMaxSize               maximum number of the connections in the connection pool. If provided, a connection pool is created
   * @param poolMaxWaitTime           maximum waiting time of a connection in the connection pool. If provided, a connection pool is created
   * @param poolMaxConnectionLifeTime maximum lifetime duration of a connection in the connection pool. If provided, a connection pool is created
   * @return A MongoDB client instance
   */
  def apply(appName: String, host: String, port: Int, dbName: String, user: Option[String], password: Option[String], authDbName: Option[String],
            poolMinSize: Option[Int], poolMaxSize: Option[Int], poolMaxWaitTime: Option[Long], poolMaxConnectionLifeTime: Option[Long]): MongoDB = {

    logger.info("Configuring the MongoDB client")
    var clientSettingsBuilder = MongoClientSettings.builder()

    //Set hostname
    clientSettingsBuilder = clientSettingsBuilder
      .applicationName(appName)
      .applyToClusterSettings(b => b.hosts(List(new ServerAddress(host, port)).asJava))
    logger.debug("MongoDB for {} is configured on {}:{}", host, port, appName)

    //If database is secure
    if (user.isDefined && password.isDefined && authDbName.isDefined) {
      clientSettingsBuilder = clientSettingsBuilder.credential(
        MongoCredential.createCredential(user.get, authDbName.get, password.get.toCharArray)
      )
      logger.debug("MongoDB is configured WITH credentials -- username:{} password:{}", user.get, password.get)
    } else {
      logger.debug("MongoDB is configured WITHOUT credentials.")
    }

    //If pooling is configured
    if (poolMinSize.isDefined || poolMaxSize.isDefined || poolMaxWaitTime.isDefined || poolMaxConnectionLifeTime.isDefined) {
      clientSettingsBuilder = clientSettingsBuilder.applyToConnectionPoolSettings(b => b.applySettings(
        ConnectionPoolSettings
          .builder()
          .minSize(poolMinSize.getOrElse(5))
          .maxSize(poolMaxSize.getOrElse(20))
          .maxWaitTime(poolMaxWaitTime.getOrElse(180L), TimeUnit.SECONDS) // 3 minutes default
          .maxConnectionLifeTime(poolMaxConnectionLifeTime.getOrElse(1200L), TimeUnit.SECONDS) // 20 minutes default
          .build()
      ))
      logger.debug("MongoDB is configured WITH connection pooling.")
    } else {
      logger.debug("MongoDB is configured WITHOUT connection pooling.")
    }

    val mongo = new MongoDB(clientSettingsBuilder.build(), dbName)
    logger.info("MongoDB is up and running!")
    mongo
  }

}
