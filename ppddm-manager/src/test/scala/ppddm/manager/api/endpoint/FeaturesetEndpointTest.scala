package ppddm.manager.api.endpoint

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.Authorization
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import org.json4s.DefaultFormats
import org.json4s.jackson.parseJson
import org.json4s.jackson.Serialization.write
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import ppddm.core.rest.model.Featureset
import ppddm.manager.PPDDMManagerEndpointTest
import ppddm.manager.config.ManagerConfig

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class FeaturesetEndpointTest extends PPDDMManagerEndpointTest {
  implicit val formats: DefaultFormats.type = DefaultFormats

  val featuresetRequestStr: String = Source.fromInputStream(getClass.getResourceAsStream("/featureset.json")).mkString

  val featuresetRequest: Featureset = parseJson(featuresetRequestStr).extract[Featureset]

  var createdFeatureset: Featureset = _

  sequential

  "Featureset Endpoint" should {
    "reject the request without a token" in {
      Post("/" + ManagerConfig.baseUri + "/featureset", HttpEntity(ContentTypes.`application/json`, featuresetRequestStr)) ~> routes ~> check {
        status shouldEqual Unauthorized
      }
    }

    "create a new feature set" in {
      Post("/" + ManagerConfig.baseUri + "/featureset", HttpEntity(ContentTypes.`application/json`, featuresetRequestStr)) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        createdFeatureset = parseJson(responseAs[String]).extract[Featureset]
        createdFeatureset.project_id shouldEqual featuresetRequest.project_id
      }
      Get("/" + ManagerConfig.baseUri + "/featureset/" + createdFeatureset.featureset_id.get) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Featureset = parseJson(responseAs[String]).extract[Featureset]
        response.featureset_id.get === createdFeatureset.featureset_id.get
        response.project_id === createdFeatureset.project_id
        response.name === createdFeatureset.name
      }
    }

    "update the feature set" in {
      val updatedFeatureset: Featureset = createdFeatureset.copy(name = "updated name")
      Put("/" + ManagerConfig.baseUri + "/featureset/" + updatedFeatureset.featureset_id.get, HttpEntity(ContentTypes.`application/json`, write(updatedFeatureset))) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Featureset = parseJson(responseAs[String]).extract[Featureset]
        response.name === updatedFeatureset.name
      }
    }

    "return all feature sets of project" in {
      // Add another feature set with the same project id
      var newFeaturesetID: String = ""
      Post("/" + ManagerConfig.baseUri + "/featureset", HttpEntity(ContentTypes.`application/json`, featuresetRequestStr)) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Featureset = parseJson(responseAs[String]).extract[Featureset]
        newFeaturesetID = response.featureset_id.get
      }
      Get("/" + ManagerConfig.baseUri + "/featureset?project_id=" + createdFeatureset.project_id) ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Seq[Featureset] = parseJson(responseAs[String]).extract[Seq[Featureset]]
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
