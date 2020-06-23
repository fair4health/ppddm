package ppddm.manager.gateway.api.endpoint

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RejectionHandler
import ppddm.core.rest.directive.CorsHandler
import ppddm.manager.controller.auth.AuthenticationController
import ppddm.manager.gateway.api.directive.ManagerExceptionHandler

trait ManagerEndpoint extends CorsHandler with ManagerExceptionHandler with ProjectEndpoint with FeaturesetEndpoint with QueryEndpoint with DataMiningEndpoint {
  def mainRoute(baseUri: String) =
    handleCORS { // CORS handling
      handleRejections(RejectionHandler.default) { // Default rejection handling
        handleExceptions(ppddmExceptionHandler) { // Exception Handling specific to PPDDM Agent
          pathPrefix(baseUri) { // Catch the baseUri at this point and do not use the baseUri in the paths of the inner routes
            authenticateOAuth2(realm = baseUri, AuthenticationController.accessTokenAuthenticator) { implicit accessToken =>
              projectRoute ~ featuresetRoute ~ queryRoute ~ dataMiningRoute
            }
          }
        }
      }
    }
}
