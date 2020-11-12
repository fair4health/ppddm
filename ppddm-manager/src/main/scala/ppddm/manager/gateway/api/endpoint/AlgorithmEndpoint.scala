package ppddm.manager.gateway.api.endpoint

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ppddm.core.rest.model.Json4sSupport._
import ppddm.manager.controller.algorithm.AlgorithmController

trait AlgorithmEndpoint {

  def algorithmRoute(implicit accessToken: String): Route = {
    pathPrefix("algorithm") {
      pathEndOrSingleSlash {
        get { // Get all available algorithms
          complete {
            AlgorithmController.getAvailableAlgorithms()
          }
        }
      }
    }
  }

}
