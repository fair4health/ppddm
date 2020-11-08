package ppddm.manager.config

import ppddm.core.config.AppConfig

import scala.util.Try

object ManagerConfig extends AppConfig {

    /** Application name */
    override lazy val appName: String = Try(config.getString("app.name")).getOrElse("FAIR4Health PPDDM-Manager")

    /** Base URI for mantIQ Services to be served from */
    override lazy val baseUri: String = Try(config.getString("server.base-uri")).getOrElse("manager")

    /** MogoDB Configuration */
    override lazy val mongoDbName: String = Try(config.getString("mongodb.db")).getOrElse("ppddm-manager")

    /** Which Agents are defined to be connected */
    lazy val agentsDefinitionPath: String = Try(config.getString("agents.definition-path")).getOrElse("agents.json")

}
