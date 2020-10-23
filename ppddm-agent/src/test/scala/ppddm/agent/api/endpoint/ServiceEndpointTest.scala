package ppddm.agent.api.endpoint

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.Specs2RouteTest
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import ppddm.agent.config.AgentConfig
import ppddm.agent.gateway.api.endpoint.AgentEndpoint

@RunWith(classOf[JUnitRunner])
class ServiceEndpointTest extends Specification with Specs2RouteTest with AgentEndpoint {

  val bearerToken: OAuth2BearerToken = OAuth2BearerToken("some-token")
  val routes: Route = mainRoute(AgentConfig.baseUri)

  sequential

  "Service Endpoint" should {
    "reject unauthorized request" in {
      Get("/" + AgentConfig.baseUri + "/metadata") ~> routes ~> check {
        status shouldEqual Unauthorized
      }
    }

    "return metadata" in {
      Get("/" + AgentConfig.baseUri + "/metadata") ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response = responseAs[String]
        response shouldEqual "\"This is GET:metadata\""
      }
    }

    "return service metadata" in {
      Get("/" + AgentConfig.baseUri + "/service/metadata") ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response = responseAs[String]
        response shouldEqual "\"This is GET:service/metadata\""
      }
    }
  }

}
