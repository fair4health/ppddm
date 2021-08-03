package ppddm.manager.config

import ppddm.core.config.AppConfig

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import ppddm.core.util.DurationConverters._
import scala.util.Try

object ManagerConfig extends AppConfig {

    /** Application name */
    override lazy val appName: String = Try(config.getString("app.name")).getOrElse("FAIR4Health PPDDM-Manager")

    /** Base URI for mantIQ Services to be served from */
    override lazy val baseUri: String = Try(config.getString("server.base-uri")).getOrElse("manager")

    /** Authentication & Authorization */
    lazy val authServerHost: String = Try(config.getString("auth.server.host")).getOrElse("default-host")
    lazy val authServerUsername: String = Try(config.getString("auth.server.username")).getOrElse("default-user")
    lazy val authServerPassword: String = Try(config.getString("auth.server.password")).getOrElse("default-password")
    lazy val loginPath: String = Try(config.getString("auth.server.login.path")).getOrElse("default-path")
    lazy val introspectionPath: String = Try(config.getString("auth.server.introspection.path")).getOrElse("default-path")

    /** MongoDB Configuration */
    override lazy val mongoDbName: String = Try(config.getString("mongodb.db")).getOrElse("ppddm-manager")

    /** Which Agents are defined to be connected */
    lazy val agentsDefinitionPath: String = Try(config.getString("agents.definition-path")).getOrElse("agents.json")

    /** What is the duration to wait before asking the results to the Agents at each cycle (for the asynchronous operations such as data-mining-model training, validation and testing) */
    lazy val orchestratorScheduleInterval: Duration  = Try(config.getDuration("dm.orchestrator.schedule.interval").asScala).getOrElse(Duration.create(120, TimeUnit.SECONDS))

}
