package ppddm.agent.api.endpoint

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.Authorization
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import ppddm.agent.PPDDMAgentEndpointTest
import ppddm.agent.config.AgentConfig

@RunWith(classOf[JUnitRunner])
class ServiceEndpointTest extends PPDDMAgentEndpointTest {

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
