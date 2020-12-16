package ppddm.manager.api.endpoint

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.Authorization
import akka.http.scaladsl.testkit.RouteTestTimeout
import scala.concurrent.duration._
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import ppddm.core.rest.model.{PredictionRequest, PredictionResult, ProspectiveStudy}
import ppddm.manager.PPDDMManagerEndpointTest
import ppddm.manager.config.ManagerConfig
import ppddm.core.rest.model.Json4sSupport._

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class ProspectiveStudyEndpointTest extends PPDDMManagerEndpointTest {

  implicit val timeout = RouteTestTimeout(30.seconds) // "make a prediction" endpoint is synchronous and takes some time to finish. Hence timeout is increased in here

  import ppddm.core.util.JsonFormatter._

  lazy val predictionRequest: PredictionRequest =
    Source.fromResource("prospective-prediction-request.json").mkString
      .extract[PredictionRequest]

  lazy val prospectiveStudy: ProspectiveStudy =
    Source.fromResource("prospective-study.json").mkString
      .extract[ProspectiveStudy]


  var predictionResult: PredictionResult = _
  var createdProspectiveStudy: ProspectiveStudy = _

  sequential

  "ProspectiveStudy Endpoint" should {
    "reject the request without a token" in {
      Post("/" + ManagerConfig.baseUri + "/predict", predictionRequest) ~> routes ~> check {
        status shouldEqual Unauthorized
      }
    }

    "make a prediction" in {
      Post("/" + ManagerConfig.baseUri + "/predict", predictionRequest) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        predictionResult = responseAs[PredictionResult]
        predictionResult.identifier shouldEqual predictionRequest.identifier
        predictionResult.variables.length shouldEqual predictionRequest.variables.length
      }
    }

    "create a new prospective study" in {
      Post("/" + ManagerConfig.baseUri + "/prospective", prospectiveStudy) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual Created

        createdProspectiveStudy = responseAs[ProspectiveStudy]
        createdProspectiveStudy.data_mining_model.model_id shouldEqual prospectiveStudy.data_mining_model.model_id
      }
      Get("/" + ManagerConfig.baseUri + "/prospective/" + createdProspectiveStudy.prospective_study_id.get) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: ProspectiveStudy = responseAs[ProspectiveStudy]
        response.prospective_study_id.get === createdProspectiveStudy.prospective_study_id.get
        response.data_mining_model.model_id === createdProspectiveStudy.data_mining_model.model_id
        response.name === createdProspectiveStudy.name
      }
    }

    "update the prospective study" in {
      val updatedProspectiveStudy: ProspectiveStudy = createdProspectiveStudy.copy(name = "updated model name")
      Put("/" + ManagerConfig.baseUri + "/prospective/" + updatedProspectiveStudy.prospective_study_id.get, updatedProspectiveStudy) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: ProspectiveStudy = responseAs[ProspectiveStudy]
        response.name === updatedProspectiveStudy.name
      }
    }

    "return all prospective studies" in {
      // Add another prospective study
      var newProspectiveStudyID: String = ""
      Post("/" + ManagerConfig.baseUri + "/prospective", prospectiveStudy) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual Created

        val response: ProspectiveStudy = responseAs[ProspectiveStudy]
        newProspectiveStudyID = response.prospective_study_id.get
      }
      Get("/" + ManagerConfig.baseUri + "/prospective") ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Seq[ProspectiveStudy] = responseAs[Seq[ProspectiveStudy]]
        response.length === 2
      }
      // Delete the newly added prospective study
      Delete("/" + ManagerConfig.baseUri + "/prospective/" + newProspectiveStudyID) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "delete the created prospective studies" in {
      Delete("/" + ManagerConfig.baseUri + "/prospective/" + createdProspectiveStudy.prospective_study_id.get) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

  }

}
