package ppddm.manager.api.endpoint

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.Authorization
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import org.json4s.DefaultFormats
import org.json4s.jackson.parseJson
import org.json4s.jackson.Serialization.write
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import ppddm.core.rest.model.Project
import ppddm.manager.PPDDMManagerEndpointTest
import ppddm.manager.config.ManagerConfig

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class ProjectEndpointTest extends PPDDMManagerEndpointTest {
  implicit val formats: DefaultFormats.type = DefaultFormats

  val projectRequestStr: String = Source.fromInputStream(getClass.getResourceAsStream("/project.json")).mkString

  val projectRequest: Project = parseJson(projectRequestStr).extract[Project]

  var createdProject: Project = _

  sequential

  "Project Endpoint" should {
    "create a new project" in {
      Post("/" + ManagerConfig.baseUri + "/project", HttpEntity(ContentTypes.`application/json`, projectRequestStr)) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        createdProject = parseJson(responseAs[String]).extract[Project]
        createdProject.name === projectRequest.name
      }
      Get("/" + ManagerConfig.baseUri + "/project/" + createdProject.project_id.get) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Project = parseJson(responseAs[String]).extract[Project]
        response.project_id.get === createdProject.project_id.get
        response.name === createdProject.name
        response.description === createdProject.description
      }
    }

    "update the project" in {
      val updatedProject: Project = createdProject.copy(name = "updated name")
      Put("/" + ManagerConfig.baseUri + "/project/" + updatedProject.project_id.get, HttpEntity(ContentTypes.`application/json`, write(updatedProject))) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Project = parseJson(responseAs[String]).extract[Project]
        response.name === updatedProject.name
      }
    }

    "list all projects created" in {
      Get("/" + ManagerConfig.baseUri + "/project") ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Seq[Project] = parseJson(responseAs[String]).extract[Seq[Project]]
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
