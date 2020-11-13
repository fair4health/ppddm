package ppddm.manager.gateway.api.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ppddm.core.rest.model.Featureset
import ppddm.core.rest.model.Json4sSupport._
import ppddm.manager.controller.featureset.FeaturesetController

import scala.concurrent.ExecutionContext.Implicits.global

trait FeaturesetEndpoint {

  def featuresetRoute(implicit accessToken: String): Route = {
    pathPrefix("featureset") {
      pathEndOrSingleSlash {
        post { // create a new feature set
          entity(as[Featureset]) { featureset =>
            complete {
              FeaturesetController.createFeatureset(featureset) map { createdFeatureset =>
                StatusCodes.Created -> createdFeatureset
              }
            }
          }
        } ~
        get { // get all feature sets of project
          parameters('project_id) { project_id =>
            complete {
              FeaturesetController.getAllFeaturesets(project_id)
            }
          }
        }
      }
    } ~
    pathPrefix("featureset" / Segment) { featureset_id =>
      pathEndOrSingleSlash {
        get { // get feature set
          complete {
            FeaturesetController.getFeatureset(featureset_id)
          }
        } ~
        put { // update feature set
          entity(as[Featureset]) { featureset =>
            complete {
              FeaturesetController.updateFeatureset(featureset)
            }
          }
        } ~
        delete { // delete feature set
          complete {
            FeaturesetController.deleteFeatureset(featureset_id)
          }
        }
      }
    }
  }
}
