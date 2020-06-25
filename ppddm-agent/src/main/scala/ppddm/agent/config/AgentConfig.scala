package ppddm.agent.config

import ppddm.core.config.AppConfig

import scala.util.Try

object AgentConfig extends AppConfig {

  /** Application name */
  override lazy val appName: String = Try(config.getString("app.name")).getOrElse("FAIR4Health PPDDM-Agent")

  /** Base URI for mantIQ Services to be served from */
  override lazy val baseUri: String = Try(config.getString("server.base-uri")).getOrElse("agent")

  /** FHIR endpoint */
  lazy val fhirEndpoint: String = Try(config.getString("fhir.endpoint")).getOrElse("http://localhost/fhir")

}
