package ppddm.core.config

import com.typesafe.config.ConfigFactory

import scala.util.Try

class AppConfig {
  protected val config = ConfigFactory.load()

  /** Application name */
  lazy val appName: String = Try(config.getString("app.name")).getOrElse("FAIR4Health PPDDM")

  /** Host name/address to start service on. */
  lazy val serverHost: String = Try(config.getString("server.host")).getOrElse("::0")

  /** Port to start service on. */
  lazy val serverPort: Int = Try(config.getInt("server.port")).getOrElse(8000)

  /** Base URI for mantIQ Services to be served from */
  lazy val baseUri: String = Try(config.getString("server.base-uri")).getOrElse("ppddm")

  /** Master url of the Spark cluster */
  lazy val sparkMaster: String = Try(config.getString("spark.master")).getOrElse("local[4]")

  /** MongoDB Configuration */
  lazy val mongoEmbedded: Boolean = Try(config.getBoolean("mongodb.embedded")).getOrElse(false)
  lazy val mongoHost: String = Try(config.getString("mongodb.host")).getOrElse("localhost")
  lazy val mongoPort: Int = Try(config.getInt("mongodb.port")).getOrElse(27017)
  lazy val mongoDbName: String = Try(config.getString("mongodb.db")).getOrElse("ppddm")
  lazy val mongoDbDrop: Boolean = Try(config.getBoolean("mongodb.drop")).getOrElse(false)

  lazy val mongoAuthDbName: Option[String] = Try(config.getString("mongodb.authdb")).toOption
  lazy val mongodbUser: Option[String] = Try(config.getString("mongodb.username")).toOption
  lazy val mongodbPassword: Option[String] = Try(config.getString("mongodb.password")).toOption

  lazy val mongoPoolingMinSize: Option[Int] = Try(config.getInt("mongodb.pooling.minSize")).toOption
  lazy val mongoPoolingMaxSize: Option[Int] = Try(config.getInt("mongodb.pooling.maxSize")).toOption
  lazy val mongoPoolingMaxWaitTime: Option[Long] = Try(config.getLong("mongodb.pooling.maxWaitTime")).toOption
  lazy val mongoPoolingMaxConnectionLifeTime: Option[Long] = Try(config.getLong("mongodb.pooling.maxConnectionLifeTime")).toOption

}
