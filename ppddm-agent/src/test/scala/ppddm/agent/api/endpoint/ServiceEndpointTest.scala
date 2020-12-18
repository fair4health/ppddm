package ppddm.agent.api.endpoint

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.Authorization
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import ppddm.agent.PPDDMAgentEndpointTest
import ppddm.agent.config.AgentConfig
import ppddm.core.rest.model.Agent

import ppddm.core.rest.model.Json4sSupport._

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
      Get("/" + AgentConfig.baseUri + "/metadata") ~> Authorization(basicHttpCredentials) ~> routes ~> check {
        status shouldEqual OK

        val response = responseAs[Agent]
        response.endpoint shouldEqual s"${AgentConfig.serverHost}:${AgentConfig.serverPort}:${AgentConfig.baseUri}"
      }
    }
  }

}
