package ppddm.manager.config

import ppddm.core.config.AppConfig

import scala.util.Try

object ManagerConfig extends AppConfig {

  object AgentConfig extends AppConfig {

    /** Application name */
    override lazy val appName: String = Try(config.getString("app.name")).getOrElse("FAIR4Health PPDDM-Manager")

    /** Base URI for mantIQ Services to be served from */
    override lazy val baseUri: String = Try(config.getString("server.base-uri")).getOrElse("manager")

  }

}
