package ppddm.manager.gateway.api.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ppddm.core.rest.model.Json4sSupport._
import ppddm.core.rest.model.{PredictionRequest, ProspectiveStudy}
import ppddm.manager.controller.prospective.ProspectiveStudyController

import scala.concurrent.ExecutionContext.Implicits.global

trait ProspectiveStudyEndpoint {

  def prospectiveStudyRoute(implicit accessToken: String): Route = {
    pathPrefix("prospective") {
      pathEndOrSingleSlash {
        post {
          entity(as[ProspectiveStudy]) { prospectiveStudy =>
            complete {
              ProspectiveStudyController.createProspectiveStudy(prospectiveStudy) map { createdProspectiveStudy =>
                StatusCodes.Created -> createdProspectiveStudy
              }
            }
          }
        } ~
          get {
            complete {
              ProspectiveStudyController.getAllProspectiveStudies
            }
          }
      }
    } ~
      pathPrefix("prospective" / Segment) { prospective_study_id =>
        pathEndOrSingleSlash {
          get {
            complete {
              ProspectiveStudyController.getProspectiveStudy(prospective_study_id)
            }
          } ~
            put {
              entity(as[ProspectiveStudy]) { prospectiveStudy =>
                complete {
                  ProspectiveStudyController.updateProspectiveStudy(prospectiveStudy)
                }
              }
            } ~
            delete {
              complete {
                ProspectiveStudyController.deleteProspectiveStudy(prospective_study_id)
              }
            }
        }
      } ~
      pathPrefix("predict") {
        pathEndOrSingleSlash {
          post {
            entity(as[PredictionRequest]) { predictionRequest =>
              complete {
                ProspectiveStudyController.predict(predictionRequest)
              }
            }
          }
        }
      }
  }

}
