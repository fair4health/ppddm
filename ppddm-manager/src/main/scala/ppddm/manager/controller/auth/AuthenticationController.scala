package ppddm.manager.controller.auth

import akka.http.scaladsl.server.directives.Credentials
import com.typesafe.scalalogging.Logger

object AuthenticationController {

  private val logger: Logger = Logger(this.getClass)

  def accessTokenAuthenticator(credentials: Credentials): Option[String] = {
    credentials match {
      case Credentials.Provided(accessToken) if checkAccessToken(accessToken) => Some(accessToken)
      case _ => None
    }
  }

  private def checkAccessToken(accessToken: String): Boolean = {
    // TODO: Introspect the provided accessToken through FAIR4Health OAuth server
    // TODO: Use Akka Cache to prevent unnecessary introspection requests
    logger.debug("AuthenticationController always return true, FOR NOW!")
    true
  }

}
