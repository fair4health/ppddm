package ppddm.manager.api.endpoint

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.Authorization
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import ppddm.core.rest.model.Project
import ppddm.manager.PPDDMManagerEndpointTest
import ppddm.manager.config.ManagerConfig

import ppddm.core.rest.model.Json4sSupport._

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class ProjectEndpointTest extends PPDDMManagerEndpointTest {

  import ppddm.core.util.JsonFormatter._

  val projectRequest: Project =
    Source.fromResource("project.json").mkString
      .extract[Project]

  var createdProject: Project = _

  sequential

  "Project Endpoint" should {

    "create a new project" in {
      Post("/" + ManagerConfig.baseUri + "/project", projectRequest) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual Created

        createdProject = responseAs[Project]
        createdProject.name === projectRequest.name
      }
      Get("/" + ManagerConfig.baseUri + "/project/" + createdProject.project_id.get) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Project = responseAs[Project]
        response.project_id.get === createdProject.project_id.get
        response.name === createdProject.name
        response.description === createdProject.description
      }
    }

    "update the project" in {
      val updatedProject: Project = createdProject.copy(name = "updated name")
      Put("/" + ManagerConfig.baseUri + "/project/" + updatedProject.project_id.get, updatedProject) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Project = responseAs[Project]
        response.name === updatedProject.name
      }
    }

    "list all projects created" in {
      Get("/" + ManagerConfig.baseUri + "/project") ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Seq[Project] = responseAs[Seq[Project]]
        response.length === 1
      }
    }

    "delete the project" in {
      Delete("/" + ManagerConfig.baseUri + "/project/" + createdProject.project_id.get) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }
  }
}
