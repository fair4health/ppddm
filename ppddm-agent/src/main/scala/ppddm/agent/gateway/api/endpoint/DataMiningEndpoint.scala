package ppddm.agent.gateway.api.endpoint

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

trait DataMiningEndpoint extends ClassificationEndpoint with ARLEndpoint {

  val dataMiningRoute: Route = {
    pathPrefix("dm") {
      classificationRoute ~ arlRoute
    }
  }
}
