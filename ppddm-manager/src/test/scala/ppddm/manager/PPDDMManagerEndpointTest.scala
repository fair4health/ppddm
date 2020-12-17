package ppddm.manager

import java.util.concurrent.TimeUnit
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.Specs2RouteTest
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll
import ppddm.core.auth.AuthManager
import ppddm.manager.gateway.api.endpoint.ManagerEndpoint
import ppddm.manager.config.ManagerConfig

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object ManagerSetup  {
  lazy val init: Unit = {
    Manager.start()
  }
  lazy val cleanup: Unit = {
    Await.result(Manager.mongoDB.getDatabase.drop().head(), Duration.apply(5, TimeUnit.SECONDS))
  }
}

trait PPDDMManagerEndpointTest extends Specification with Specs2RouteTest with BeforeAfterAll with ManagerEndpoint {
  // Start PPDDM Manager
  override def beforeAll(): Unit = {
    ManagerSetup.init
    bearerToken = OAuth2BearerToken(AuthManager.getAccessToken.getOrElse("some-test-token-which-will-be-ignored"))
    routes = mainRoute(ManagerConfig.baseUri)
  }
  // Drop the database created for tests
  override def afterAll(): Unit = {
    ManagerSetup.cleanup
  }

  // Some token does not matter for the endpoint test
  var bearerToken: OAuth2BearerToken = _
  // Manager endpoint routes
  var routes: Route = _
}
