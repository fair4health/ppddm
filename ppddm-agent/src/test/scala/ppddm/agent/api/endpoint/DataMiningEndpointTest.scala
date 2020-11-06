package ppddm.agent.api.endpoint

import java.time
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.Cancellable
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.Authorization
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import org.json4s.DefaultFormats
import org.json4s.jackson.parseJson
import org.json4s.jackson.Serialization.write
import org.junit.runner.RunWith
import org.specs2.matcher.MatchResult
import org.specs2.runner.JUnitRunner
import ppddm.agent.PPDDMAgentEndpointTest
import ppddm.agent.config.AgentConfig
import ppddm.core.rest.model.{BoostedModel, DataPreparationRequest, ModelTestRequest, ModelTrainingRequest, ModelTrainingResult, ModelValidationRequest}

import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.io.Source

@RunWith(classOf[JUnitRunner])
class DataMiningEndpointTest extends PPDDMAgentEndpointTest {
  implicit val formats: DefaultFormats.type = DefaultFormats

  val dataPreparationRequestStr: String = Source.fromInputStream(getClass.getResourceAsStream("/data-preparation-requests/request-with-variables.json")).mkString
  val modelTrainingRequestStr1: String = Source.fromInputStream(getClass.getResourceAsStream("/model-training-requests/request1.json")).mkString
  val modelTrainingRequestStr2: String = Source.fromInputStream(getClass.getResourceAsStream("/model-training-requests/request2.json")).mkString
  val modelTestRequestStr: String = Source.fromInputStream(getClass.getResourceAsStream("/model-test-requests/request1.json")).mkString

  val dataPreparationRequest: DataPreparationRequest = parseJson(dataPreparationRequestStr).extract[DataPreparationRequest]
  val modelTrainingRequests: Seq[ModelTrainingRequest] = Seq(
    parseJson(modelTrainingRequestStr1).extract[ModelTrainingRequest],
    parseJson(modelTrainingRequestStr2).extract[ModelTrainingRequest]
  )
  var modelTrainingResult: ModelTrainingResult = _
  var modelValidationRequests: Seq[ModelValidationRequest] = Seq.empty[ModelValidationRequest]
  var modelTestRequest: ModelTestRequest = parseJson(modelTestRequestStr).extract[ModelTestRequest]


  /**
   * Asks model training result repeatedly with a scheduler and when it is ready, prepares the validation and test requests
   * @param model_id Requested model id
   * @return
   */
  def askForTrainingResult(model_id: String): MatchResult[Boolean] = {
    val askForTrainingResultPromise: Promise[Done] = Promise[Done]
    var askForTrainingResultScheduler: Option[Cancellable] = None

    // Set a scheduler to ask if model training result is ready
    askForTrainingResultScheduler = Some(actorSystem.scheduler.scheduleWithFixedDelay(
      time.Duration.ZERO,
      time.Duration.ofSeconds(2),
      () => {
        Get("/" + AgentConfig.baseUri + "/dm/train/" + model_id) ~> Authorization(bearerToken) ~> routes ~> check {
          if (status.intValue() == 200) {
            // Parse model training result
            modelTrainingResult = parseJson(responseAs[String]).extract[ModelTrainingResult]

            // Create model validation request body from training result
            modelValidationRequests = modelValidationRequests :+ ModelValidationRequest(
              model_id = modelTrainingResult.model_id,
              dataset_id = modelTrainingResult.dataset_id,
              agent = modelTrainingResult.agent,
              weak_models = modelTrainingResult.algorithm_training_models,
              submitted_by = "test")

            // Create model test request body for only model1
            if (model_id == "model1") {
              val boostedModels: Seq[BoostedModel] = Seq(
                BoostedModel(
                  algorithm = modelTestRequest.boosted_models.head.algorithm,
                  weak_models = Seq(modelTrainingResult.algorithm_training_models.head.copy(weight = Some(1.0))),
                  test_statistics = None,
                  calculated_test_statistics = None,
                  selection_status = None
                ))
              modelTestRequest = ModelTestRequest(modelTestRequest.model_id, modelTestRequest.dataset_id, modelTestRequest.agent, boostedModels)
            }

            // Complete promise and cancel the scheduler
            askForTrainingResultPromise.success(Done)
            askForTrainingResultScheduler.get.cancel()
          }
        }
      },
      actorSystem.dispatcher
    ))
    // Try 10 times at 2-second intervals
    askForTrainingResultPromise.isCompleted must be_==(true).eventually(10, Duration(4, TimeUnit.SECONDS))
  }

  /**
   * Asks model validation result repeatedly with a scheduler
   * @param model_id Requested model id
   * @return
   */
  def askForValidationResult(model_id: String): MatchResult[Boolean] = {
    val askForValidationResultPromise: Promise[Done] = Promise[Done]
    var askForValidationResultScheduler: Option[Cancellable] = None

    // Set a scheduler to ask if model validation result is ready
    askForValidationResultScheduler = Some(actorSystem.scheduler.scheduleWithFixedDelay(
      time.Duration.ZERO,
      time.Duration.ofSeconds(2),
      () => {
        Get("/" + AgentConfig.baseUri + "/dm/validate/" + model_id) ~> Authorization(bearerToken) ~> routes ~> check {
          if (status.intValue() == 200) {
            askForValidationResultPromise.success(Done)
            askForValidationResultScheduler.get.cancel()
          }
        }
      },
      actorSystem.dispatcher
    ))
    // Try 10 times at 2-second intervals
    askForValidationResultPromise.isCompleted must be_==(true).eventually(10, Duration(4, TimeUnit.SECONDS))
  }

  sequential

  "Data Mining Endpoint" should {
    "create a dataset first" in {
      Post("/" + AgentConfig.baseUri + "/prepare", HttpEntity(ContentTypes.`application/json`, dataPreparationRequestStr)) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }

      val askForDatasetPromise: Promise[Done] = Promise[Done]
      var askForDatasetScheduler: Option[Cancellable] = None

      // Set a scheduler to ask if dataset and statistics are ready
      askForDatasetScheduler = Some(actorSystem.scheduler.scheduleWithFixedDelay(
        time.Duration.ZERO,
        time.Duration.ofSeconds(2),
        () => {
          Get("/" + AgentConfig.baseUri + "/prepare/" + dataPreparationRequest.dataset_id) ~> Authorization(bearerToken) ~> routes ~> check {
            if (status.intValue() == 200) {
              // Complete promise and cancel the scheduler
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

    "start model training - request1" in {
      Post("/" + AgentConfig.baseUri + "/dm/train", HttpEntity(ContentTypes.`application/json`, modelTrainingRequestStr1)) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "start model training - request2" in {
      Post("/" + AgentConfig.baseUri + "/dm/train", HttpEntity(ContentTypes.`application/json`, modelTrainingRequestStr2)) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "ask for model training results whether it is ready or not" in {
      modelTrainingRequests.map { modelTrainingRequest =>
        askForTrainingResult(modelTrainingRequest.model_id)
      }
    }

    "validate the model" in {
      modelValidationRequests.map { modelValidationRequest =>
        Post("/" + AgentConfig.baseUri + "/dm/validate", HttpEntity(ContentTypes.`application/json`, write(modelValidationRequest))) ~> Authorization(bearerToken) ~> routes ~> check {
          status shouldEqual OK
        }
      }
    }

    "ask for model validation results" in {
      modelValidationRequests.map { modelValidationRequest =>
        askForValidationResult(modelValidationRequest.model_id)
      }
    }

    "test the boosted model" in {
      Post("/" + AgentConfig.baseUri + "/dm/test", HttpEntity(ContentTypes.`application/json`, write(modelTestRequest))) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "ask for the boosted model test result" in {
      val askForTestResultPromise: Promise[Done] = Promise[Done]
      var askForTestResultScheduler: Option[Cancellable] = None

      // Set a scheduler to ask if dataset and statistics are ready
      askForTestResultScheduler = Some(actorSystem.scheduler.scheduleWithFixedDelay(
        time.Duration.ZERO,
        time.Duration.ofSeconds(2),
        () => {
          Get("/" + AgentConfig.baseUri + "/dm/test/" + modelTestRequest.model_id) ~> Authorization(bearerToken) ~> routes ~> check {
            if (status.intValue() == 200) {
              // Complete promise and cancel the scheduler
              askForTestResultPromise.success(Done)
              askForTestResultScheduler.get.cancel()
            }
          }
        },
        actorSystem.dispatcher
      ))
      // Try 10 times at 2-second intervals
      askForTestResultPromise.isCompleted must be_==(true).eventually(10, Duration(4, TimeUnit.SECONDS))
    }

    "delete the created dataset and statistics" in {
      Delete("/" + AgentConfig.baseUri + "/prepare/" + dataPreparationRequest.dataset_id) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "delete the training model" in {
      modelTrainingRequests.map { modelTrainingRequest =>
        Delete("/" + AgentConfig.baseUri + "/dm/train/" + modelTrainingRequest.model_id) ~> Authorization(bearerToken) ~> routes ~> check {
          status shouldEqual OK
        }
      }
    }

    "delete the validation result" in {
      modelValidationRequests.map { modelValidationRequest =>
        Delete("/" + AgentConfig.baseUri + "/dm/validate/" + modelValidationRequest.model_id) ~> Authorization(bearerToken) ~> routes ~> check {
          status shouldEqual OK
        }
      }
    }

    "delete the model test result" in {
      Delete("/" + AgentConfig.baseUri + "/dm/test/" + modelTestRequest.model_id) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }
  }
}
