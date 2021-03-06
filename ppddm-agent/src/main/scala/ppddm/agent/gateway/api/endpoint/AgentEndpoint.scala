package ppddm.agent.gateway.api.endpoint

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RejectionHandler
import ppddm.agent.controller.auth.AuthenticationController
import ppddm.agent.gateway.api.directive.AgentExceptionHandler
import ppddm.core.rest.directive.CorsHandler

trait AgentEndpoint extends CorsHandler with AgentExceptionHandler with ServiceEndpoint with DataPreparationEndpoint with DataMiningEndpoint {

  def mainRoute(baseUri: String) =
    handleCORS { // CORS handling
      handleRejections(RejectionHandler.default) { // Default rejection handling
        rejectEmptyResponse { // Reject the empty responses
          handleExceptions(ppddmExceptionHandler) { // Exception Handling specific to PPDDM Agent
            pathPrefix(baseUri) { // Catch the baseUri at this point and do not use the baseUri in the paths of the inner routes
              authenticateBasic(realm = baseUri, AuthenticationController.idSecretAuthenticator) { _ =>
                prepareRoute ~ serviceRoute ~ dataMiningRoute
              }
            }
          }
        }
      }
    }

}
