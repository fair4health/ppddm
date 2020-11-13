package ppddm.manager.api.endpoint

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.Authorization
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import ppddm.core.rest.model.DataMiningModel
import ppddm.manager.PPDDMManagerEndpointTest
import ppddm.manager.config.ManagerConfig
import ppddm.core.rest.model.Json4sSupport._

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class DataMiningModelEndpointTest extends PPDDMManagerEndpointTest {

  import ppddm.core.util.JsonFormatter._

  val bareDataMiningModel: DataMiningModel =
    Source.fromResource("dataminingmodel.json").mkString
      .extract[DataMiningModel]

  val fullDataMiningModel: DataMiningModel =
    Source.fromResource("dataminingmodel-with-boostedmodels.json").mkString
      .extract[DataMiningModel]

  var createdBareDataMiningModel: DataMiningModel = _
  var createdFullDataMiningModel: DataMiningModel = _

  sequential

  "DataMiningModel Endpoint" should {
    "reject the request without a token" in {
      Post("/" + ManagerConfig.baseUri + "/dm-model", bareDataMiningModel) ~> routes ~> check {
        status shouldEqual Unauthorized
      }
    }

    "create a bare new data mining model" in {
      Post("/" + ManagerConfig.baseUri + "/dm-model?_test", bareDataMiningModel) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual Created

        createdBareDataMiningModel = responseAs[DataMiningModel]
        createdBareDataMiningModel.project_id shouldEqual bareDataMiningModel.project_id
        createdBareDataMiningModel.dataset.dataset_id.get shouldEqual bareDataMiningModel.dataset.dataset_id.get
      }
      Get("/" + ManagerConfig.baseUri + "/dm-model/" + createdBareDataMiningModel.model_id.get) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: DataMiningModel = responseAs[DataMiningModel]
        response.model_id.get === createdBareDataMiningModel.model_id.get
        response.project_id === createdBareDataMiningModel.project_id
        response.name === createdBareDataMiningModel.name
      }
    }

    "create a full new data mining model" in {
      Post("/" + ManagerConfig.baseUri + "/dm-model?_test", fullDataMiningModel) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual Created

        createdFullDataMiningModel = responseAs[DataMiningModel]
        createdFullDataMiningModel.project_id shouldEqual fullDataMiningModel.project_id
        createdFullDataMiningModel.dataset.dataset_id.get shouldEqual fullDataMiningModel.dataset.dataset_id.get
        createdFullDataMiningModel.boosted_models.get.length shouldEqual fullDataMiningModel.boosted_models.get.length
      }
      Get("/" + ManagerConfig.baseUri + "/dm-model/" + createdFullDataMiningModel.model_id.get) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: DataMiningModel = responseAs[DataMiningModel]
        response.model_id.get === createdFullDataMiningModel.model_id.get
        response.project_id === createdFullDataMiningModel.project_id
        response.name === createdFullDataMiningModel.name
      }
    }

    "update the data mining model" in {
      val updatedDataMiningModel: DataMiningModel = createdBareDataMiningModel.copy(name = "updated model name")
      Put("/" + ManagerConfig.baseUri + "/dm-model/" + updatedDataMiningModel.model_id.get, updatedDataMiningModel) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: DataMiningModel = responseAs[DataMiningModel]
        response.name === updatedDataMiningModel.name
      }
    }

    "return all data mining models of project" in {
      // Add another data mining model with the same project id
      var newModelID: String = ""
      Post("/" + ManagerConfig.baseUri + "/dm-model?_test", bareDataMiningModel) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual Created

        val response: DataMiningModel = responseAs[DataMiningModel]
        newModelID = response.model_id.get
      }
      Get("/" + ManagerConfig.baseUri + "/dm-model?project_id=" + createdBareDataMiningModel.project_id) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Seq[DataMiningModel] = responseAs[Seq[DataMiningModel]]
        response.length === 3
      }
      // Delete the newly added data mining model
      Delete("/" + ManagerConfig.baseUri + "/dm-model/" + newModelID) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "delete the created data mining models" in {
      Delete("/" + ManagerConfig.baseUri + "/dm-model/" + createdBareDataMiningModel.model_id.get) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
      Delete("/" + ManagerConfig.baseUri + "/dm-model/" + createdFullDataMiningModel.model_id.get) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

  }

}
