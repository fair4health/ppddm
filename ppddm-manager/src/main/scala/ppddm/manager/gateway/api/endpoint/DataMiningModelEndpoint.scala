package ppddm.manager.gateway.api.endpoint

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ppddm.core.rest.model.DataMiningModel
import ppddm.manager.controller.dm.DataMiningController
import ppddm.core.rest.model.Json4sSupport._

trait DataMiningModelEndpoint {

  def dataMiningModelRoute(implicit accessToken: String): Route = {
    pathPrefix("dm-model") {
      pathEndOrSingleSlash {
        post {
          entity(as[DataMiningModel]) { datamining_model =>
            complete {
              datamining_model // Create a new DataMiningModel and return the created entity
            }
          }
        } ~
          get {
            complete {
              Seq.empty[DataMiningModel] // Get all DataMiningModel for the 1st screen of the mockups (for Model Management)
            }
          }
      }
    } ~
      pathPrefix("dm-model" / Segment) { model_id =>
        pathEndOrSingleSlash {
          get {
            complete {
              null // Retrieve the DataMiningModel
            }
          } ~
            put {
              entity(as[DataMiningModel]) { datamining_model =>
                complete {
                  datamining_model // Update the DataMiningModel (to select an AlgorithmExecution) and return the updated entity
                }
              }
            } ~
            delete {
              complete {
                null // Delete the DataMiningModel
              }
            }
        }
      }
  }

}
