package ppddm.manager.api.endpoint

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.Authorization
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import ppddm.core.rest.model.Dataset
import ppddm.manager.PPDDMManagerEndpointTest
import ppddm.manager.config.ManagerConfig
import ppddm.core.rest.model.Json4sSupport._

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class DatasetEndpointTest extends PPDDMManagerEndpointTest {

  import ppddm.core.util.JsonFormatter._

  val bareDataset: Dataset =
    Source.fromResource("dataset.json").mkString
      .extract[Dataset]

  val fullDataset: Dataset =
    Source.fromResource("dataset-with-datasetsources.json").mkString
      .extract[Dataset]

  var createdBareDataset: Dataset = _
  var createdFullDataset: Dataset = _

  sequential

  "Dataset Endpoint" should {
    "reject the request without a token" in {
      Post("/" + ManagerConfig.baseUri + "/dataset", bareDataset) ~> routes ~> check {
        status shouldEqual Unauthorized
      }
    }

    "create a bare new dataset set" in {
      Post("/" + ManagerConfig.baseUri + "/dataset?_test", bareDataset) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual Created

        createdBareDataset = responseAs[Dataset]
        createdBareDataset.project_id shouldEqual bareDataset.project_id
        createdBareDataset.featureset.featureset_id.get shouldEqual bareDataset.featureset.featureset_id.get
      }
      Get("/" + ManagerConfig.baseUri + "/dataset/" + createdBareDataset.dataset_id.get + "?_test") ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Dataset = responseAs[Dataset]
        response.dataset_id.get === createdBareDataset.dataset_id.get
        response.project_id === createdBareDataset.project_id
        response.name === createdBareDataset.name
      }
    }

    "create a full new dataset set" in {
      Post("/" + ManagerConfig.baseUri + "/dataset?_test", fullDataset) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual Created

        createdFullDataset = responseAs[Dataset]
        createdFullDataset.project_id shouldEqual fullDataset.project_id
        createdFullDataset.featureset.featureset_id.get shouldEqual fullDataset.featureset.featureset_id.get
        createdFullDataset.dataset_sources.get.length shouldEqual fullDataset.dataset_sources.get.length
      }
      Get("/" + ManagerConfig.baseUri + "/dataset/" + createdFullDataset.dataset_id.get + "?_test") ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Dataset = responseAs[Dataset]
        response.dataset_id.get === createdFullDataset.dataset_id.get
        response.project_id === createdFullDataset.project_id
        response.name === createdFullDataset.name
      }
    }

    "update the dataset set" in {
      val updatedDataset: Dataset = createdBareDataset.copy(name = "updated dataset name")
      Put("/" + ManagerConfig.baseUri + "/dataset/" + updatedDataset.dataset_id.get + "?_test", updatedDataset) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Dataset = responseAs[Dataset]
        response.name === updatedDataset.name
      }
    }

    "return all datasets of project" in {
      // Add another dataset with the same project id
      var newDatasetID: String = ""
      Post("/" + ManagerConfig.baseUri + "/dataset?_test", bareDataset) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual Created

        val response: Dataset = responseAs[Dataset]
        newDatasetID = response.dataset_id.get
      }
      Get("/" + ManagerConfig.baseUri + "/dataset?_test&project_id=" + createdBareDataset.project_id) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Seq[Dataset] = responseAs[Seq[Dataset]]
        response.length === 3
      }
      // Delete the newly added dataset set
      Delete("/" + ManagerConfig.baseUri + "/dataset/" + newDatasetID + "?_test") ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "delete the created datasets" in {
      Delete("/" + ManagerConfig.baseUri + "/dataset/" + createdBareDataset.dataset_id.get + "?_test") ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
      Delete("/" + ManagerConfig.baseUri + "/dataset/" + createdFullDataset.dataset_id.get + "?_test") ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

  }
}