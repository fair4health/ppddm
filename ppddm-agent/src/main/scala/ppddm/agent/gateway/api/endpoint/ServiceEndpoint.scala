package ppddm.agent.gateway.api.endpoint

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ppddm.agent.controller.service.ServiceController
import ppddm.core.rest.model.Json4sSupport._

trait ServiceEndpoint {

  val serviceRoute: Route = {
    pathPrefix("metadata") {
      pathEndOrSingleSlash {
        get {
          complete {
            ServiceController.getMetadata
          }
        }
      }
    }
  }

}
