package ppddm.agent.gateway.api.endpoint

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ppddm.agent.controller.dm.DataMiningController
import ppddm.core.rest.model.Json4sSupport._

trait DataMiningEndpoint {

  def dataMiningRoute(implicit accessToken: String): Route = {
    pathPrefix("dm") {
      pathPrefix("regression") {
        pathEndOrSingleSlash {
          get {
            complete {
              DataMiningController.testRegression()
            }
          }
        }
      }
    }
  }

}
