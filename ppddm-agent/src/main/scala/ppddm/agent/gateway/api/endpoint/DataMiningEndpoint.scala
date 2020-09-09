package ppddm.agent.gateway.api.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ppddm.core.rest.model.Json4sSupport._
import ppddm.core.rest.model.{AlgorithmExecutionRequest, AlgorithmExecutionResult}

trait DataMiningEndpoint {

  def dataMiningRoute(implicit accessToken: String): Route = {
    pathPrefix("dm") {
      pathEndOrSingleSlash {
        post {
          entity(as[AlgorithmExecutionRequest]) { algorithmExecutionRequest =>
            complete {
              StatusCodes.OK // Accept the request, start the execution and return immediately (since this is asynchronous)
            }
          }
        }
      }
    } ~
      pathPrefix("dm" / Segment) { model_id =>
        pathEndOrSingleSlash {
          get {
            complete {
              Option.empty[AlgorithmExecutionResult]
            }
          } ~
            delete {
              complete {
                StatusCodes.OK
              }
            }
        }
      }
  }

}
