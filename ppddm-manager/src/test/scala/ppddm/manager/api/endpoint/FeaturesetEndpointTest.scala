package ppddm.manager.api.endpoint

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.Authorization
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import ppddm.core.rest.model.Featureset
import ppddm.manager.PPDDMManagerEndpointTest
import ppddm.manager.config.ManagerConfig

import ppddm.core.rest.model.Json4sSupport._

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class FeaturesetEndpointTest extends PPDDMManagerEndpointTest {

  import ppddm.core.util.JsonFormatter._

  val featuresetRequest: Featureset =
    Source.fromInputStream(getClass.getResourceAsStream("/featureset.json")).mkString
      .extract[Featureset]

  var createdFeatureset: Featureset = _

  sequential

  "Featureset Endpoint" should {

    "reject the request without a token" in {
      Post("/" + ManagerConfig.baseUri + "/featureset", featuresetRequest) ~> routes ~> check {
        status shouldEqual Unauthorized
      }
    }

    "create a new feature set" in {
      Post("/" + ManagerConfig.baseUri + "/featureset", featuresetRequest) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        createdFeatureset = responseAs[Featureset]
        createdFeatureset.project_id shouldEqual featuresetRequest.project_id
      }
      Get("/" + ManagerConfig.baseUri + "/featureset/" + createdFeatureset.featureset_id.get) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Featureset = responseAs[Featureset]
        response.featureset_id.get === createdFeatureset.featureset_id.get
        response.project_id === createdFeatureset.project_id
        response.name === createdFeatureset.name
      }
    }

    "update the feature set" in {
      val updatedFeatureset: Featureset = createdFeatureset.copy(name = "updated name")
      Put("/" + ManagerConfig.baseUri + "/featureset/" + updatedFeatureset.featureset_id.get, updatedFeatureset) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Featureset = responseAs[Featureset]
        response.name === updatedFeatureset.name
      }
    }

    "return all feature sets of project" in {
      // Add another feature set with the same project id
      var newFeaturesetID: String = ""
      Post("/" + ManagerConfig.baseUri + "/featureset", featuresetRequest) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Featureset = responseAs[Featureset]
        newFeaturesetID = response.featureset_id.get
      }
      Get("/" + ManagerConfig.baseUri + "/featureset?project_id=" + createdFeatureset.project_id) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Seq[Featureset] = responseAs[Seq[Featureset]]
        response.length === 2
      }
      // Delete the newly added feature set
      Delete("/" + ManagerConfig.baseUri + "/featureset/" + newFeaturesetID) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }

    "delete the created feature set" in {
      Delete("/" + ManagerConfig.baseUri + "/featureset/" + createdFeatureset.featureset_id.get) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK
      }
    }
  }
}
