package ppddm.agent.api.endpoint

import java.time
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.Cancellable
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.Authorization
import org.junit.runner.RunWith
import org.specs2.matcher.MatchResult
import org.specs2.runner.JUnitRunner
import ppddm.agent.PPDDMAgentEndpointTest
import ppddm.agent.config.AgentConfig
import ppddm.core.rest.model.{BoostedModel, DataPreparationRequest, ModelTestRequest, ModelTrainingRequest, ModelTrainingResult, ModelValidationRequest}

import ppddm.core.rest.model.Json4sSupport._

import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.io.Source

@RunWith(classOf[JUnitRunner])
class DataMiningEndpointTest extends PPDDMAgentEndpointTest {

  import ppddm.core.util.JsonFormatter._

  lazy val dataPreparationRequest: DataPreparationRequest =
    Source.fromInputStream(getClass.getResourceAsStream("/data-preparation-requests/data-preparation-request.json")).mkString
      .extract[DataPreparationRequest]
  lazy val modelTrainingRequest1: ModelTrainingRequest =
    Source.fromInputStream(getClass.getResourceAsStream("/model-training-requests/model-training-request1.json")).mkString
      .extract[ModelTrainingRequest]
  lazy val modelTrainingRequest2: ModelTrainingRequest =
    Source.fromInputStream(getClass.getResourceAsStream("/model-training-requests/model-training-request2.json")).mkString
      .extract[ModelTrainingRequest]

  var modelTrainingResult: ModelTrainingResult = _
  var modelTestRequest: ModelTestRequest = _
  var modelValidationRequest1: ModelValidationRequest = _
  var modelValidationRequest2: ModelValidationRequest = _

  /**
   * Asks model training result repeatedly with a scheduler and when it is ready, prepares the validation and test requests
   *
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
        Get("/" + AgentConfig.baseUri + "/dm/classification/train/" + model_id) ~> Authorization(bearerToken) ~> routes ~> check {
          if (status == OK) {
            // Parse model training result
            modelTrainingResult = responseAs[ModelTrainingResult]

            if (model_id == "model1") {
              // Create model validation request body from training result
              modelValidationRequest1 = ModelValidationRequest(
                model_id = modelTrainingResult.model_id,
                dataset_id = modelTrainingResult.dataset_id,
                agent = modelTrainingResult.agent,
                weak_models = modelTrainingResult.algorithm_training_models,
                submitted_by = "test")

              // Create model test request body for only model1
              val boostedModels: Seq[BoostedModel] = Seq(
                BoostedModel(
                  algorithm = modelTrainingRequest1.algorithms.head,
                  weak_models = Seq(modelTrainingResult.algorithm_training_models.head.copy(weight = Some(1.0))),
                  combined_frequent_items = None,
                  combined_total_record_count = None,
                  combined_association_rules = None,
                  test_statistics = None,
                  calculated_test_statistics = None,
                  selection_status = None
                ))
              modelTestRequest = ModelTestRequest(modelTrainingResult.model_id, modelTrainingResult.dataset_id, modelTrainingResult.agent, boostedModels, "test")
            } else if (model_id == "model2") {
              // Create model validation request body from training result
              modelValidationRequest2 = ModelValidationRequest(
                model_id = modelTrainingResult.model_id,
                dataset_id = modelTrainingResult.dataset_id,
                agent = modelTrainingResult.agent,
                weak_models = modelTrainingResult.algorithm_training_models,
                submitted_by = "test")
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
   *
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
        Get("/" + AgentConfig.baseUri + "/dm/classification/validate/" + model_id) ~> Authorization(bearerToken) ~> routes ~> check {
          if (status == OK) {
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
      Post("/" + AgentConfig.baseUri + "/prepare", dataPreparationRequest) ~> Authorization(bearerToken) ~> routes ~> check {
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
            if (status == OK) {
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
      Post("/" + AgentConfig.baseUri + "/dm/classification/train", modelTrainingRequest1) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "ask for model training results - request1" in {
      askForTrainingResult(modelTrainingRequest1.model_id)
    }

    "start model training - request2" in {
      Post("/" + AgentConfig.baseUri + "/dm/classification/train", modelTrainingRequest2) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "ask for model training results - request2" in {
      askForTrainingResult(modelTrainingRequest2.model_id)
    }

    "start model validation - request 1" in {
      Post("/" + AgentConfig.baseUri + "/dm/classification/validate", modelValidationRequest1) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "ask for model validation results - request1" in {
      askForValidationResult(modelValidationRequest1.model_id)
    }

    "start model validation - request 2" in {
      Post("/" + AgentConfig.baseUri + "/dm/classification/validate", modelValidationRequest2) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "ask for model validation results - request2" in {
      askForValidationResult(modelValidationRequest2.model_id)
    }


    "test the boosted model - request 1" in {
      Post("/" + AgentConfig.baseUri + "/dm/classification/test", modelTestRequest) ~> Authorization(bearerToken) ~> routes ~> check {
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
          Get("/" + AgentConfig.baseUri + "/dm/classification/test/" + modelTestRequest.model_id) ~> Authorization(bearerToken) ~> routes ~> check {
            if (status == OK) {
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

    "delete the training results" in {
      Delete("/" + AgentConfig.baseUri + "/dm/classification/train/" + modelTrainingRequest1.model_id) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
      Delete("/" + AgentConfig.baseUri + "/dm/classification/train/" + modelTrainingRequest2.model_id) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "delete the validation results" in {
      Delete("/" + AgentConfig.baseUri + "/dm/classification/validate/" + modelValidationRequest1.model_id) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
      Delete("/" + AgentConfig.baseUri + "/dm/classification/validate/" + modelValidationRequest2.model_id) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "delete the model test result" in {
      Delete("/" + AgentConfig.baseUri + "/dm/classification/test/" + modelTestRequest.model_id) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "delete the created dataset and statistics" in {
      Delete("/" + AgentConfig.baseUri + "/prepare/" + dataPreparationRequest.dataset_id) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }
  }
}
