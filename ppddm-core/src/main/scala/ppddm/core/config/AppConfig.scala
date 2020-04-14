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
  lazy val sparkMaster = Try(config.getString("spark.master")).getOrElse("local[4]")
}
