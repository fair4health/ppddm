package ppddm.agent

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.Specs2RouteTest
import org.apache.spark.sql.SparkSession
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll
import ppddm.agent.config.AgentConfig
import ppddm.core.ai.DataMiningEngine
import ppddm.agent.gateway.api.endpoint.AgentEndpoint

trait PPDDMAgentEndpointTest extends Specification with Specs2RouteTest with BeforeAfterAll with AgentEndpoint {
  // Init Data mining engine and sparkSession before all tests
  override def beforeAll(): Unit = Agent.dataMiningEngine = DataMiningEngine(AgentConfig.appName, AgentConfig.sparkMaster)
  // After all tests, close the spark session
//  override def afterAll(): Unit = Agent.dataMiningEngine.sparkSession.close()

  implicit val actorSystem: ActorSystem = ActorSystem("ppddm-agent-test")
  lazy implicit val sparkSession: SparkSession = Agent.dataMiningEngine.sparkSession

  // Some token does not matter for the endpoint test
  val bearerToken: OAuth2BearerToken = OAuth2BearerToken("some-test-token")
  // Agent endpoint routes
  val routes: Route = mainRoute(AgentConfig.baseUri)
}
