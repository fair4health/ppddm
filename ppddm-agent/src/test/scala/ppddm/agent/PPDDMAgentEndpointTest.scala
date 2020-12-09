package ppddm.agent

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.Specs2RouteTest
import org.apache.spark.sql.SparkSession
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll
import ppddm.agent.config.AgentConfig
import ppddm.core.ai.DataMiningEngine
import ppddm.agent.gateway.api.endpoint.AgentEndpoint
import ppddm.core.fhir.FHIRClient
import ppddm.core.util.URLUtil

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.io.Source

object AgentSetup {
  implicit lazy val actorSystem: ActorSystem = ActorSystem("ppddm-agent-test")
  lazy val dataMiningEngine: DataMiningEngine = DataMiningEngine(AgentConfig.appName, AgentConfig.sparkMaster)

  lazy val init: Unit = {
    Agent.dataMiningEngine = this.dataMiningEngine
    populateFHIRWithDemoData()
  }

  lazy val cleanup: Unit = {
    deleteFHIRWithDemoData()
  }

  lazy val fhirServerBaseURI: String = URLUtil.append(s"http://${AgentConfig.fhirHost}:${AgentConfig.fhirPort}", AgentConfig.fhirBaseUri)
  lazy val fhirResourcesPut: String =
    Source.fromInputStream(getClass.getResourceAsStream("/fhir-resources/put-fhir-resources.json")).mkString
  lazy val fhirResourcesDelete: String = {
    Source.fromInputStream(getClass.getResourceAsStream("/fhir-resources/delete-fhir-resources.json")).mkString
  }

  lazy val fhirClient: FHIRClient = FHIRClient(AgentConfig.fhirHost, AgentConfig.fhirPort, AgentConfig.fhirBaseUri, AgentConfig.fhirProtocol)

  private def populateFHIRWithDemoData(): Unit = {
    println("The FHIR Repository will be populated with test resources.")
    Await.result(fhirClient.postBundle(fhirResourcesPut), Duration.apply(5, TimeUnit.SECONDS))
    println("The FHIR Repository has been successfully populated with test resources.")
  }

  private def deleteFHIRWithDemoData(): Unit = {
    println("Test resources will be deleted from the FHIR Repository.")
    Await.result(fhirClient.postBundle(fhirResourcesDelete), Duration.apply(5, TimeUnit.SECONDS))
    println("Test resources have been deleted from the FHIR Repository.")
  }
}

trait PPDDMAgentEndpointTest extends Specification with Specs2RouteTest with BeforeAfterAll with AgentEndpoint {

  implicit lazy val actorSystem: ActorSystem = AgentSetup.actorSystem
  implicit lazy val sparkSession: SparkSession = AgentSetup.dataMiningEngine.sparkSession

  // Some token does not matter for the endpoint test
  val bearerToken: OAuth2BearerToken = OAuth2BearerToken("some-test-token")
  // Agent endpoint routes
  val routes: Route = mainRoute(AgentConfig.baseUri)

  // Init Data mining engine and sparkSession before all tests
  override def beforeAll(): Unit = {
    AgentSetup.init
  }

  // After all tests, close the spark session
  override def afterAll(): Unit = {
    AgentSetup.cleanup
  }

}
