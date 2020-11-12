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
class DataMiningModelEndpointTest extends PPDDMManagerEndpointTest {

//  import ppddm.core.util.JsonFormatter._
//
//  val datasetRequest: Dataset =
//    Source.fromInputStream(getClass.getResourceAsStream("/featureset.json")).mkString
//      .extract[Dataset]
//
//  var createdFeatureset: Featureset = _
//
//  sequential
//
//  "DataMiningModel Endpoint" should {
//    "reject the request without a token" in {
//      Post("/" + ManagerConfig.baseUri + "/dataset", featuresetRequest) ~> routes ~> check {
//        status shouldEqual Unauthorized
//      }
//    }
//  }
}
