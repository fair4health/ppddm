package ppddm.agent.gateway.api.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ppddm.agent.controller.dm.DataMiningController
import ppddm.core.rest.model.Json4sSupport._
import ppddm.core.rest.model.{AlgorithmExecutionRequest, AlgorithmExecutionResult}

trait DataMiningEndpoint {

  def dataMiningRoute(implicit accessToken: String): Route = {
    pathPrefix("dm") {
      pathEndOrSingleSlash {
        post { // Submit a new algorithm execution request to this Agent
          entity(as[AlgorithmExecutionRequest]) { algorithmExecutionRequest =>
            complete { // Accept the request, start the execution and return immediately (since this is asynchronous)
              DataMiningController.startAlgorithmExecution(algorithmExecutionRequest) // Start the algorithm execution and continue
              StatusCodes.OK // Return the successful result after the execution starts (do not wait for the result)
            }
          }
        }
      }
    } ~
    pathPrefix("dm" / Segment) { model_id =>
      pathEndOrSingleSlash {
        get {
          complete { // Get the result of algorithm execution. This returns 404 if it is not ready yet.
            DataMiningController.getAlgorithmExecutionResult(model_id)
          }
        } ~
          delete { // Delete the AlgorithmExecutionResult
            complete {
              DataMiningController.deleteAlgorithmExecutionResult(model_id)
            }
          }
      }
    }
  }
}
