package ppddm.agent.gateway.api.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ppddm.agent.controller.dm.ARLController
import ppddm.core.rest.model.{ARLExecutionRequest, ARLFrequencyCalculationRequest}
import ppddm.core.rest.model.Json4sSupport._

trait ARLEndpoint {

  val arlRoute: Route = {
    pathPrefix("arl" / "frequency") {
      pathEndOrSingleSlash {
        post { // Submit a frequency calculation request to this Agent
          entity(as[ARLFrequencyCalculationRequest]) { frequencyCalculationRequest =>
            complete { // Accept the request, start the frequency calculation and return immediately (since this is asynchronous)
              ARLController.startFrequencyCalculation(frequencyCalculationRequest) // Start the frequency calculation and continue
              StatusCodes.OK // Return the successful result after the calculation starts (do not wait for the result)
            }
          }
        }
      }
    } ~
    pathPrefix("arl" / "frequency" / Segment) { model_id =>
      pathEndOrSingleSlash {
        get {
          complete { // Get the result of frequency calculation. This returns 404 if it is not ready yet.
            ARLController.getFrequencyCalculationResult(model_id)
          }
        } ~
          delete { // Delete the ARLFrequencyCalculationResult
            complete {
              ARLController.deleteFrequencyCalculationResult(model_id)
            }
          }
      }
    } ~
      pathPrefix("arl" / "execute") {
        pathEndOrSingleSlash {
          post { // Submit a execution request to this Agent
            entity(as[ARLExecutionRequest]) { arlExecutionRequest =>
              complete { // Accept the request, start the execution and return immediately (since this is asynchronous)
                ARLController.startExecution(arlExecutionRequest) // Start the execution and continue
                StatusCodes.OK // Return the successful result after the execution starts (do not wait for the result)
              }
            }
          }
        }
      } ~
      pathPrefix("arl" / "execute" / Segment) { model_id =>
        pathEndOrSingleSlash {
          get {
            complete { // Get the result of execution. This returns 404 if it is not ready yet.
              ARLController.getExecutionResult(model_id)
            }
          } ~
            delete { // Delete the ARLExecutionResult
              complete {
                ARLController.deleteExecutionResult(model_id)
              }
            }
        }
      }
  }

}
