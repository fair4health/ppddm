package ppddm.agent.controller.auth

import akka.http.scaladsl.server.directives.Credentials
import com.typesafe.scalalogging.Logger
import ppddm.agent.config.AgentConfig

object AuthenticationController {

  private val logger: Logger = Logger(this.getClass)

  def idSecretAuthenticator(credentials: Credentials): Option[String] = {
    credentials match {
      case p @ Credentials.Provided(id) if checkIDSecret(p) => Some(id)
      case _ => None
    }
  }

  private def checkIDSecret(p: Credentials.Provided): Boolean = {
    if(!AgentConfig.authEnabled) {
      // If the authentication is not enabled, return true for all given id-secrets
      logger.debug("Authentication is not enabled, so I am authenticating the user with whatever is provided as id and secret.")
      return true
    }

    p.identifier == AgentConfig.authClientID && p.verify(AgentConfig.authClientSecret)
  }
}
