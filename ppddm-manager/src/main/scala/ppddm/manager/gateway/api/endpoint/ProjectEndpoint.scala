package ppddm.manager.gateway.api.endpoint

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ppddm.core.rest.model.Json4sSupport._
import ppddm.core.rest.model.Project
import ppddm.manager.controller.project.ProjectController

trait ProjectEndpoint {

  def projectRoute(implicit accessToken: String): Route = {
    pathPrefix("project") {
      pathEndOrSingleSlash {
        post {
          entity(as[Project]) { project =>
            complete {
              ProjectController.createProject(project)
            }
          }
        } ~
        get {
          complete {
            ProjectController.getAllProjects
          }
        }
      }
    } ~
    pathPrefix("project" / Segment) { project_id =>
      pathEndOrSingleSlash {
        get {
          complete {
            ProjectController.getProject(project_id)
          }
        } ~
        put {
          entity(as[Project]) { project =>
            complete {
              ProjectController.updateProject(project)
            }
          }
        } ~
        delete {
          complete {
            ProjectController.deleteProject(project_id)
          }
        }
      }
    }
  }

}
