package ppddm.manager

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.Specs2RouteTest
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll
import ppddm.manager.gateway.api.endpoint.ManagerEndpoint
import ppddm.manager.config.ManagerConfig

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object ManagerSetup  {
  lazy val env: Unit = Manager.start()
}

trait PPDDMManagerEndpointTest extends Specification with Specs2RouteTest with BeforeAfterAll with ManagerEndpoint {
  // Start PPDDM Manager
  override def beforeAll(): Unit = ManagerSetup.env
  // Drop the database created for tests
  override def afterAll(): Unit = Await.result(Manager.mongoDB.getDatabase.drop().head(), Duration.apply(5, TimeUnit.SECONDS))

  // Some token does not matter for the endpoint test
  val bearerToken: OAuth2BearerToken = OAuth2BearerToken("some-test-token")
  // Manager endpoint routes
  val routes: Route = mainRoute(ManagerConfig.baseUri)
}
