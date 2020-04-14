package ppddm.manager.gateway.api.endpoint

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ppddm.manager.controller.query.QueryController

/**
 * Handles the Queries made by the Platform
 */
trait QueryEndpoint {

  def queryRoute(implicit accessToken: String): Route = {
    pathPrefix("query") {
      pathEndOrSingleSlash {
        get {
          complete {
            QueryController.query("This is GET:query")
          }
        }
        //~
//        post {
//          entity(as[QueryObject]) { kpi => // QueryObject: EligibilityQuery:String + FeatureSet
//            complete {
//              QueryController.
//            }
//          }
//        }
      }
    }
  }

}
