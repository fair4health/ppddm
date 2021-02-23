package ppddm.agent.gateway.api.endpoint

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ppddm.agent.controller.prepare.DataPreparationController
import ppddm.core.rest.model.DataPreparationRequest
import ppddm.core.rest.model.Json4sSupport._

/**
 * Handles the Data preparation (data extraction) requests submitted by the ppddm-manager
 */
trait DataPreparationEndpoint {

  val prepareRoute: Route = {
    pathPrefix("prepare") {
      pathEndOrSingleSlash {
        post { // Submit a new data preparation request to this Agent
          entity(as[DataPreparationRequest]) { dataPreparationRequest =>
            complete {
              DataPreparationController.startPreparation(dataPreparationRequest) // Start the preparation and continue
              StatusCodes.OK // Return the successful result after the preparation starts (do not wait for the result)
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
        } ~
          delete { // Delete the prepared data from this Agent
            complete {
              DataPreparationController.deleteData(dataset_id)
            }
          }
      }
    }
  }

}
