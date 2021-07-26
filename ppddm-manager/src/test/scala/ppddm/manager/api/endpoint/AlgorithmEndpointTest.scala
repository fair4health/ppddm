package ppddm.manager.api.endpoint

import akka.http.scaladsl.model.headers.Authorization
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import ppddm.manager.PPDDMManagerEndpointTest
import ppddm.manager.config.ManagerConfig
import akka.http.scaladsl.model.StatusCodes._
import ppddm.core.rest.model.{Algorithm, AlgorithmName}
import ppddm.core.rest.model.Json4sSupport._

@RunWith(classOf[JUnitRunner])
class AlgorithmEndpointTest extends PPDDMManagerEndpointTest {

  sequential

  "Algorithms Endpoint" should {

    "retrieve all available algorithms" in {
      Get("/" + ManagerConfig.baseUri + "/algorithm") ~> Authorization(bearerToken) ~> routes ~> check {
        status shouldEqual OK

        val response: Seq[Algorithm] = responseAs[Seq[Algorithm]]
        response.size shouldEqual 10
        // We cannot make this check because we do not create the Enumarations appropriately. The values of the Enumeration objects are not Value instances
        // response.size shouldEqual AlgorithmName.values.size
      }
    }
  }

}
