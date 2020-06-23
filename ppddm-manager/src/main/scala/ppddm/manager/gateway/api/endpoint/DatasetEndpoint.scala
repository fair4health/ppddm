package ppddm.manager.gateway.api.endpoint

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ppddm.core.rest.model.Dataset
import ppddm.core.rest.model.Json4sSupport._
import ppddm.manager.controller.dataset.DatasetController

trait DatasetEndpoint {

  def datasetRoute(implicit accessToken: String): Route = {
    pathPrefix("dataset") {
      pathEndOrSingleSlash {
        post { // create a new data set
          entity(as[Dataset]) { dataset =>
            complete {
              DatasetController.createDataset(dataset)
            }
          }
        } ~
          get { // get all data sets
            complete {
              DatasetController.getAllDatasets
            }
          }
      }
    } ~
      pathPrefix("dataset" / Segment) { dataset_id =>
        pathEndOrSingleSlash {
          get { // get data set
            complete {
              DatasetController.getDataset(dataset_id)
            }
          } ~
            put { // update data set
              entity(as[Dataset]) { dataset =>
                complete {
                  DatasetController.updateDataset(dataset)
                }
              }
            } ~
            delete { // delete data set
              complete {
                DatasetController.deleteDataset(dataset_id)
              }
            }
        }
      }
  }
}
