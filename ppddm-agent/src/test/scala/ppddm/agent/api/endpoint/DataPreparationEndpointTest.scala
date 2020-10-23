package ppddm.agent.api.endpoint

import java.time
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.Cancellable
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Route
import org.json4s._
import org.json4s.jackson.parseJson
import org.junit.runner.RunWith
import org.specs2.concurrent.ExecutionEnv
import org.specs2.runner.JUnitRunner
import ppddm.agent.PPDDMAgentTest
import ppddm.agent.config.AgentConfig
import ppddm.agent.controller.prepare.DataPreparationController
import ppddm.agent.gateway.api.endpoint.AgentEndpoint
import ppddm.core.rest.model.DataPreparationRequest

import scala.concurrent.Promise
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.io.Source

@RunWith(classOf[JUnitRunner])
class DataPreparationEndpointTest extends PPDDMAgentTest with AgentEndpoint {
  implicit val formats: DefaultFormats.type = DefaultFormats

  val dataPreparationRequestWVariables: String = Source.fromInputStream(getClass.getResourceAsStream("/data-preparation-requests/request-with-variables.json")).mkString
  val dataPreparationRequestWoutVariables: String = Source.fromInputStream(getClass.getResourceAsStream("/data-preparation-requests/request-without-variables.json")).mkString

  val dataPreparationRequest: DataPreparationRequest = parseJson(dataPreparationRequestWVariables).extract[DataPreparationRequest]

  val bearerToken: OAuth2BearerToken = OAuth2BearerToken("some-token")
  val routes: Route = mainRoute(AgentConfig.baseUri)

  val askForDatasetPromise: Promise[Int] = Promise[Int]
  var askForDatasetScheduler: Cancellable = _

  sequential

  def askForDataset(dataset_id: String): Any = {
    Get("/" + AgentConfig.baseUri + "/prepare/" + dataset_id) ~> Authorization(bearerToken) ~> routes ~> check {
      println("Asking for dataset: " + dataset_id)
      if (status.intValue() == 200) {
        askForDatasetPromise.success(1)
        askForDatasetScheduler.cancel()
      }
    }
  }

  "Data Preparation Endpoint" should {
    "reject the request without a token" in {
      Post("/" + AgentConfig.baseUri + "/prepare", HttpEntity(dataPreparationRequestWVariables)) ~> routes ~> check {
        status shouldEqual Unauthorized
      }
    }

    "reject the request without featureSet Variable" in {
      Post("/" + AgentConfig.baseUri + "/prepare", HttpEntity(ContentTypes.`application/json`, dataPreparationRequestWoutVariables)) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual BadRequest
      }
    }

    "reject to get if the statistics for dataset with given id are not ready" in {
      Get("/" + AgentConfig.baseUri + "/prepare/some-id-does-not-exist") ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual NotFound

        val response = responseAs[String]
        response shouldEqual "The requested resource could not be found."
      }
    }

//    "prepare dataset with given data preparation request" in {
//      implicit val ee: ExecutionEnv = ExecutionEnv.fromGlobalExecutionContext
//      // Start the data preparation with the given data preparation request
//      DataPreparationController.startPreparation(dataPreparationRequest) must be_==(Done).awaitFor(FiniteDuration(20, TimeUnit.SECONDS))
//      // Check if the data set and statistics have been prepared
//      Get("/" + AgentConfig.baseUri + "/prepare/" + dataPreparationRequest.dataset_id) ~> Authorization(bearerToken) ~> routes ~> check {
//        status shouldEqual OK
//      }
//    }

    "start data preparation" in {
      Post("/" + AgentConfig.baseUri + "/prepare", HttpEntity(ContentTypes.`application/json`, dataPreparationRequestWVariables)) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "ask for dataset whether it is ready or not" in {
      // Set a scheduler to ask if dataset and statistics are ready
      askForDatasetScheduler = actorSystem.scheduler.scheduleWithFixedDelay(
        time.Duration.ZERO,
        time.Duration.ofSeconds(2),
        () => {
          askForDataset(dataPreparationRequest.dataset_id)
        },
        actorSystem.dispatcher
      )
      // Try 10 times at 2-second intervals
      askForDatasetPromise.isCompleted must be_==(true).eventually(10, Duration(4, TimeUnit.SECONDS))
    }

    "delete the created dataset and statistics" in {
      Delete("/" + AgentConfig.baseUri + "/prepare/" + dataPreparationRequest.dataset_id) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "reject to delete if the dataset and statistics for given dataset_id do not exist" in {
      Delete("/" + AgentConfig.baseUri + "/prepare/some-id-does-not-exist") ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual NotFound

        val response = responseAs[String]
        response shouldEqual "The requested resource could not be found."
      }
    }
  }

}
