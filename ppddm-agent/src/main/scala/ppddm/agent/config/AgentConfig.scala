package ppddm.agent.config

import ppddm.core.config.AppConfig

import scala.util.Try

object AgentConfig extends AppConfig {

  /** Application name */
  override lazy val appName: String = Try(config.getString("app.name")).getOrElse("FAIR4Health PPDDM-Agent")

  /** Base URI for mantIQ Services to be served from */
  override lazy val baseUri: String = Try(config.getString("server.base-uri")).getOrElse("agent")

  /** FHIR endpoint */
  lazy val fhirProtocol: String = Try(config.getString("fhir.protocol")).getOrElse("http")
  lazy val fhirHost: String = Try(config.getString("fhir.host")).getOrElse("localhost")
  lazy val fhirPort: Int = Try(config.getInt("fhir.port")).getOrElse(80)
  lazy val fhirBaseUri: String = Try(config.getString("fhir.base-uri")).getOrElse("/")

  /** PPDDM-Agent config */
  lazy val agentID: String = Try(config.getString("agent.id")).getOrElse("agent-1")
  lazy val agentDeploymentEndpoint: String = Try(config.getString("agent.endpoint")).getOrElse(s"${serverHost}:${serverPort}/${baseUri}")
  lazy val agentBatchSize: Int = Try(config.getInt("agent.batch-size")).getOrElse(10)

}
