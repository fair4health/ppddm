package ppddm.manager.gateway.api.endpoint

import akka.Done
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ppddm.core.rest.model.DataMiningModel
import ppddm.manager.controller.dm.{DataMiningModelController, DataMiningOrchestrator, DistributedDataMiningManager}
import ppddm.core.rest.model.Json4sSupport._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait DataMiningModelEndpoint {

  def dataMiningModelRoute(implicit accessToken: String): Route = {
    pathPrefix("dm-model") {
      pathEndOrSingleSlash {
        post {
          parameters("_test".?) { test =>
            entity(as[DataMiningModel]) { datamining_model =>
              complete { // Create a new DataMiningModel and return the created entity
                DataMiningModelController.createDataMiningModel(datamining_model) map { dataMiningModel =>

                  if (test.isEmpty) { // Start the orchestration only if this call is NOT for testing purposes
                    // After the DataMiningModel is saved into the database, start an orchestration process for this model
                    // so that model training, validation and testing are executed through the distributed data mining approach
                    // Do not wait this orchestration to be completed, start the orchestration and return.
                    DataMiningOrchestrator.startOrchestration(dataMiningModel)
                  }

                  StatusCodes.Created -> dataMiningModel // Return the OK result, do not wait for the orchestration to finish
                }
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
              parameters("_test".?) { test =>
                complete { // Delete the DataMiningModel
                  DataMiningModelController.deleteDataMiningModel(model_id, test.isDefined)
                }
              }
            }
        }
      }
  }

}
