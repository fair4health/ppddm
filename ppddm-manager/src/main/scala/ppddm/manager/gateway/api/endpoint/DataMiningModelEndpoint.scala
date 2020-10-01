package ppddm.manager.gateway.api.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ppddm.core.rest.model.DataMiningModel
import ppddm.manager.controller.dm.DataMiningModelController
import ppddm.core.rest.model.Json4sSupport._

import scala.concurrent.ExecutionContext.Implicits.global

trait DataMiningModelEndpoint {

  def dataMiningModelRoute(implicit accessToken: String): Route = {
    pathPrefix("dm-model") {
      pathEndOrSingleSlash {
        post {
          entity(as[DataMiningModel]) { datamining_model =>
            complete { // Create a new DataMiningModel and return the created entity
              DataMiningModelController.createDataMiningModel(datamining_model) map { dataMiningModel =>
                StatusCodes.Created -> dataMiningModel
              }
            }
          }
        } ~
          get { // get all DataMiningModels of project
            parameters('project_id) { project_id =>
              complete {
                DataMiningModelController.getAllDataMiningModels(project_id)
              }
            }
          }
      }
    } ~
      pathPrefix("dm-model" / Segment) { model_id =>
        pathEndOrSingleSlash {
          get { // Retrieve the DataMiningModel
            complete {
              DataMiningModelController.getDataMiningModel(model_id)
            }
          } ~
            put {
              entity(as[DataMiningModel]) { datamining_model =>
                complete { // Update the DataMiningModel (to select an AlgorithmExecution) and return the updated entity
                  DataMiningModelController.updateDataMiningModel(datamining_model)
                }
              }
            } ~
            delete {
              complete { // Delete the DataMiningModel
                DataMiningModelController.deleteDataMiningModel(model_id)
              }
            }
        }
      }
  }

}
