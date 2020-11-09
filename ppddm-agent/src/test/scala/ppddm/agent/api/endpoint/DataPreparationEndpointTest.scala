package ppddm.agent.api.endpoint

import java.time
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.Cancellable
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.Authorization
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import ppddm.agent.PPDDMAgentEndpointTest
import ppddm.agent.config.AgentConfig
import ppddm.core.rest.model.DataPreparationRequest
import ppddm.core.util.JsonFormatter._

import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.io.Source

@RunWith(classOf[JUnitRunner])
class DataPreparationEndpointTest extends PPDDMAgentEndpointTest {

  val dataPreparationRequestWithVariables: DataPreparationRequest =
    Source.fromInputStream(getClass.getResourceAsStream("/data-preparation-requests/request-with-variables.json")).mkString
      .extract[DataPreparationRequest]
  val dataPreparationRequestWithoutVariables: DataPreparationRequest =
    Source.fromInputStream(getClass.getResourceAsStream("/data-preparation-requests/request-without-variables.json")).mkString
      .extract[DataPreparationRequest]
  val dataPreparationRequestOfZeroPatients: DataPreparationRequest =
    Source.fromInputStream(getClass.getResourceAsStream("/data-preparation-requests/request-of-zero-patients.json")).mkString
      .extract[DataPreparationRequest]

  sequential

  "Data Preparation Endpoint" should {
    "reject the request without a token" in {
      Post("/" + AgentConfig.baseUri + "/prepare", HttpEntity(dataPreparationRequestWithVariables.toJson)) ~> routes ~> check {
        status shouldEqual Unauthorized
      }
    }

    "reject the request without featureSet Variable" in {
      Post("/" + AgentConfig.baseUri + "/prepare", HttpEntity(ContentTypes.`application/json`, dataPreparationRequestWithoutVariables.toJson)) ~> Authorization(bearerToken) ~> routes ~> check {
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

    "start data preparation" in {
      Post("/" + AgentConfig.baseUri + "/prepare", HttpEntity(ContentTypes.`application/json`, dataPreparationRequestWithVariables.toJson)) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "ask for dataset whether it is ready or not" in {
      val askForDatasetPromise: Promise[Done] = Promise[Done]
      var askForDatasetScheduler: Option[Cancellable] = None
      // Set a scheduler to ask if dataset and statistics are ready
      askForDatasetScheduler = Some(actorSystem.scheduler.scheduleWithFixedDelay(
        time.Duration.ZERO,
        time.Duration.ofSeconds(2),
        () => {
          Get("/" + AgentConfig.baseUri + "/prepare/" + dataPreparationRequestWithVariables.dataset_id) ~> Authorization(bearerToken) ~> routes ~> check {
            if (status.intValue() == 200) {
              askForDatasetPromise.success(Done)
              askForDatasetScheduler.get.cancel()
            }
          }
        },
        actorSystem.dispatcher
      ))
      // Try 10 times at 2-second intervals
      askForDatasetPromise.isCompleted must be_==(true).eventually(10, Duration(4, TimeUnit.SECONDS))
    }

    "delete the created dataset and statistics" in {
      Delete("/" + AgentConfig.baseUri + "/prepare/" + dataPreparationRequestWithVariables.dataset_id) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "start data preparation for zero (0) patients" in {
      Post("/" + AgentConfig.baseUri + "/prepare", HttpEntity(ContentTypes.`application/json`, dataPreparationRequestOfZeroPatients.toJson)) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "ask for zero patient dataset whether it is ready or not" in {
      val askForDatasetPromise: Promise[Done] = Promise[Done]
      var askForDatasetScheduler: Option[Cancellable] = None
      // Set a scheduler to ask if dataset and statistics are ready
      askForDatasetScheduler = Some(actorSystem.scheduler.scheduleWithFixedDelay(
        time.Duration.ZERO,
        time.Duration.ofSeconds(2),
        () => {
          Get("/" + AgentConfig.baseUri + "/prepare/" + dataPreparationRequestOfZeroPatients.dataset_id) ~> Authorization(bearerToken) ~> routes ~> check {
            if (status.intValue() == 200) {
              askForDatasetPromise.success(Done)
              askForDatasetScheduler.get.cancel()
            }
          }
        },
        actorSystem.dispatcher
      ))
      // Try 10 times at 2-second intervals
      askForDatasetPromise.isCompleted must be_==(true).eventually(10, Duration(4, TimeUnit.SECONDS))
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
