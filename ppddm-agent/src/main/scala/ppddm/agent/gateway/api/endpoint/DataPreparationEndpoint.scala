package ppddm.agent.gateway.api.endpoint

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ppddm.agent.controller.prepare.DataPreparationController
import ppddm.core.rest.model.DataPreparationRequest
import ppddm.core.rest.model.Json4sSupport._

/**
 * Handles the Data preparation (data extraction) requests submitted by the ppddm-manager
 */
trait DataPreparationEndpoint {

  def prepareRoute(implicit accessToken: String): Route = {
    pathPrefix("prepare") {
      pathEndOrSingleSlash {
        post { // Submit a new data preparation request to this Agent
          entity(as[DataPreparationRequest]) { dataPreparationRequest =>
            complete {
              DataPreparationController.startPreparation(dataPreparationRequest)
            }
          }
        }
      }
    } ~
    pathPrefix("prepare" / Segment) { dataset_id =>
      pathEndOrSingleSlash {
        get {
          complete { // Get the prepared data source statistics. This returns 404 if it is not ready yet.
            DataPreparationController.getDataSourceStatistics(dataset_id)
          }
        }
      }
    }
  }

}
