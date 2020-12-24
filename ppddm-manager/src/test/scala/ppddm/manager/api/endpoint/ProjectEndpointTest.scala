package ppddm.manager.api.endpoint

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.Authorization
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import ppddm.core.rest.model.{DataMiningModel, Dataset, Featureset, Project, ProspectiveStudy}
import ppddm.manager.PPDDMManagerEndpointTest
import ppddm.manager.config.ManagerConfig
import ppddm.core.rest.model.Json4sSupport._

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class ProjectEndpointTest extends PPDDMManagerEndpointTest {

  import ppddm.core.util.JsonFormatter._

  lazy val projectRequest: Project =
    Source.fromResource("project.json").mkString
      .extract[Project]
  lazy val featureset: Featureset =
    Source.fromResource("featureset.json").mkString
      .extract[Featureset]
  lazy val bareDataset: Dataset =
    Source.fromResource("dataset.json").mkString
      .extract[Dataset]
  lazy val bareDataMiningModel: DataMiningModel =
    Source.fromResource("dataminingmodel.json").mkString
      .extract[DataMiningModel]
  lazy val prospectiveStudy: ProspectiveStudy =
    Source.fromResource("prospective-study.json").mkString
      .extract[ProspectiveStudy]

  var createdProject: Project = _
  var createdFeatureset: Featureset = _
  var createdDataset: Dataset = _
  var createdDataMiningModel: DataMiningModel = _
  var createdProspectiveStudy: ProspectiveStudy = _

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

    "create featureset, dataset, dataminingmodel and prospectivestudy under the project" in {
      Post("/" + ManagerConfig.baseUri + "/featureset", featureset.withNewProjectId(createdProject.project_id.get)) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual Created
        createdFeatureset = responseAs[Featureset]
        createdFeatureset.name === featureset.name
        createdFeatureset.project_id === createdProject.project_id.get
      }
      Post("/" + ManagerConfig.baseUri + "/dataset?_test", bareDataset.withNewProjectId(createdProject.project_id.get)) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual Created
        createdDataset = responseAs[Dataset]
        createdDataset.name === bareDataset.name
        createdDataset.project_id === createdProject.project_id.get
      }
      Post("/" + ManagerConfig.baseUri + "/dm-model?_test", bareDataMiningModel.withNewProjectId(createdProject.project_id.get)) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual Created
        createdDataMiningModel = responseAs[DataMiningModel]
        createdDataMiningModel.name === bareDataMiningModel.name
        createdDataMiningModel.project_id === createdProject.project_id.get
      }
      Post("/" + ManagerConfig.baseUri + "/prospective", prospectiveStudy.withNewProjectId(createdProject.project_id.get)) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual Created
        createdProspectiveStudy = responseAs[ProspectiveStudy]
        createdProspectiveStudy.name === prospectiveStudy.name
        createdProspectiveStudy.project_id === createdProject.project_id.get
      }
    }

    "delete the project with all associated resources" in {
      Delete("/" + ManagerConfig.baseUri + "/project/" + createdProject.project_id.get + "?_all&_test") ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
      Get("/" + ManagerConfig.baseUri + "/featureset/" + createdFeatureset.featureset_id.get) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual NotFound
      }
      Get("/" + ManagerConfig.baseUri + "/dataset/" + createdDataset.dataset_id.get + "?_test") ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual NotFound
      }
      Get("/" + ManagerConfig.baseUri + "/dm-model/" + createdDataMiningModel.model_id.get) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual NotFound
      }
      Get("/" + ManagerConfig.baseUri + "/prospective/" + createdProspectiveStudy.prospective_study_id.get) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual NotFound
      }
    }
  }
}
