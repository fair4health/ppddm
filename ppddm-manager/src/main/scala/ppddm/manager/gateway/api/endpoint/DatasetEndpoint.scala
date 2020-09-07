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
          entity(as[Dataset]) { dataset =>
            complete {
              StatusCodes.Created -> DatasetController.createDataset(dataset)
            }
          }
        } ~
          get { // get all data sets of project
            parameters('project_id) { project_id =>
              complete {
                DatasetController.getAllDatasets(project_id)
              }
            }
          }
      }
    } ~
      pathPrefix("dataset" / Segment) { dataset_id =>
        pathEndOrSingleSlash {
          get { // get data set
            complete {
              DatasetController.getDataset(dataset_id) map { dataset =>
                if(dataset.isDefined) StatusCodes.OK -> dataset.get
                else StatusCodes.NotFound
              }
            }
          } ~
            put { // update data set
              entity(as[Dataset]) { dataset =>
                complete {
                  DatasetController.updateDataset(dataset) map {dataset =>
                    if(dataset.isDefined) StatusCodes.OK -> dataset.get
                    else StatusCodes.NotFound
                  }
                }
              }
            } ~
            delete { // delete data set
              complete {
                DatasetController.deleteDataset(dataset_id) map {dataset =>
                  if(dataset.isDefined) StatusCodes.OK -> dataset.get
                  else StatusCodes.NotFound
                }
              }
            }
        }
      }
  }
}
