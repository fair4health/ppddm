package ppddm.agent.config

import ppddm.core.config.AppConfig

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.util.Try

object AgentConfig extends AppConfig {

  /** Application name */
  override lazy val appName: String = Try(config.getString("app.name")).getOrElse("FAIR4Health PPDDM-Agent")

  /** Base URI to be served from */
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
  lazy val dataPreparationTimeout: Duration = Try(Duration.fromNanos(config.getDuration("agent.data-preparation-timeout").toNanos)).getOrElse(Duration.create(1, TimeUnit.HOURS))
  lazy val associationMaxItemCount: Int  = Try(config.getInt("agent.dm.association.max-item-count")).getOrElse(4)

}
