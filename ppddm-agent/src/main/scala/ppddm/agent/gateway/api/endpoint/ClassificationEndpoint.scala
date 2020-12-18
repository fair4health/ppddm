package ppddm.agent.gateway.api.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ppddm.agent.controller.dm.ClassificationController
import ppddm.core.rest.model.Json4sSupport._
import ppddm.core.rest.model.{ModelTestRequest, ModelTrainingRequest, ModelValidationRequest}

trait ClassificationEndpoint {

  val classificationRoute: Route = {
    pathPrefix("classification" / "train") {
      pathEndOrSingleSlash {
        post { // Submit a new model training request to this Agent
          entity(as[ModelTrainingRequest]) { modelTrainingRequest =>
            complete { // Accept the request, start the training and return immediately (since this is asynchronous)
              ClassificationController.startTraining(modelTrainingRequest) // Start the model training and continue
              StatusCodes.OK // Return the successful result after the training starts (do not wait for the result)
            }
          }
        }
      }
    } ~
      pathPrefix("classification" / "train" / Segment) { model_id =>
        pathEndOrSingleSlash {
          get {
            complete { // Get the result of model training. This returns 404 if it is not ready yet.
              ClassificationController.getTrainingResult(model_id)
            }
          } ~
            delete { // Delete the ModelTrainingResult
              complete {
                ClassificationController.deleteTrainingResult(model_id)
              }
            }
        }
      } ~
      pathPrefix("classification" / "validate") {
        pathEndOrSingleSlash {
          post { // Submit a new model validation request to this Agent so that another Agent's trained model can be validated on the data of this Agent
            entity(as[ModelValidationRequest]) { modelValidationRequest =>
              complete { // Accept the request, start the validation and return immediately (since this is asynchronous)
                ClassificationController.startValidation(modelValidationRequest)
                StatusCodes.OK
              }
            }
          }
        }
      } ~
      pathPrefix("classification" / "validate" / Segment) { model_id =>
        pathEndOrSingleSlash {
          get {
            complete { // Get the result of model validation. This returns 404 if it is not ready yet.
              ClassificationController.getValidationResult(model_id)
            }
          } ~
            delete { // Delete the ModelValidationResult
              complete {
                ClassificationController.deleteValidationResult(model_id)
              }
            }
        }
      } ~
      pathPrefix("classification" / "test") {
        pathEndOrSingleSlash {
          post { // Submit a test request so that the BoostedModel can be tested on this Agent
            entity(as[ModelTestRequest]) { modelTestRequest =>
              complete {
                ClassificationController.startTesting(modelTestRequest)
                StatusCodes.OK
              }
            }
          }
        }
      } ~
      pathPrefix("classification" / "test" / Segment) { model_id =>
        pathEndOrSingleSlash {
          get {
            complete { // Get the result of model testing. This returns 404 if it is not ready yet.
              ClassificationController.getTestResult(model_id)
            }
          } ~
            delete { // Delete the ModelTestResult
              complete {
                ClassificationController.deleteTestResult(model_id)
              }
            }
        }
      }
  }
}
