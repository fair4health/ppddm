package ppddm.manager.gateway.api.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ppddm.core.rest.model.Dataset
import ppddm.core.rest.model.Json4sSupport._
import ppddm.manager.controller.dataset.DatasetController

import scala.concurrent.ExecutionContext.Implicits.global

trait DatasetEndpoint {

  def datasetRoute(implicit accessToken: String): Route = {
    pathPrefix("dataset") {
      pathEndOrSingleSlash {
        post { // create a new data set
          parameters("_test".?) { test =>
            entity(as[Dataset]) { dataset =>
              complete {
                DatasetController.createDataset(dataset, test.isDefined) map { createdDataset =>
                  StatusCodes.Created -> createdDataset
                }
              }
            }
          }
        } ~
          get { // get all data sets of project
            parameters('project_id, "_test".?) { (project_id, test) =>
              complete {
                DatasetController.getAllDatasets(project_id, test.isDefined)
              }
            }
          }
      }
    } ~
    pathPrefix("dataset" / Segment) { dataset_id =>
      pathEndOrSingleSlash {
        get { // get data set
          parameters("_test".?) { test =>
            complete {
              DatasetController.getDataset(dataset_id, test.isDefined)
            }
          }
        } ~
          put { // update data set
            parameters("_test".?) { test =>
              entity(as[Dataset]) { dataset =>
                complete {
                  DatasetController.updateDataset(dataset, test.isDefined)
                }
              }
            }
          } ~
          delete { // delete data set
            parameters("_test".?) { test =>
              complete {
                DatasetController.deleteDataset(dataset_id, test.isDefined)
              }
            }
          }
      }
    } ~
    pathPrefix("xdataset" / Segment) { dataset_id =>
      pathEndOrSingleSlash {
        get {
          complete {
            DatasetController.getXDataset(Some(dataset_id))
          }
        }
      }
    }
  }
}
