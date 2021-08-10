package ppddm.manager.controller.auth

import akka.http.scaladsl.server.directives.Credentials
import com.typesafe.scalalogging.Logger
import ppddm.core.auth.AuthManager
import ppddm.core.exception.AuthException
import ppddm.manager.config.ManagerConfig

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

object AuthenticationController {

  private val logger: Logger = Logger(this.getClass)

  def accessTokenAuthenticator(credentials: Credentials): Option[String] = {
    credentials match {
      case Credentials.Provided(accessToken) if checkAccessToken(accessToken) => Some(accessToken)
      case _ => None
    }
  }

  private def checkAccessToken(accessToken: String): Boolean = {
    if(!ManagerConfig.authEnabled) {
      // If the authentication is not enabled, return true for all given accessTokens, do not interact with the auth server.
      //logger.debug("Authentication is not enabled, so I am authenticating the user by default.")
      return true
    }

    // Introspect the provided accessToken through onAuth server using Akka Cache
    val authResponse = AuthManager.getOrLoad(accessToken,
      accessToken ⇒ AuthManager.introspect(accessToken)).map { authContext =>
      if (authContext.isExpired) {
        AuthManager.remove(accessToken)
        logger.debug(s"AuthContext expired for accessToken:$accessToken")
        false
      } else {
        logger.debug(s"Authenticated! AuthContext successfully accessed for accessToken:$accessToken")
        true
      }
    }

    try { // Wait for the result to decide the validity of the accessToken
      Await.result(authResponse, Duration(10, TimeUnit.SECONDS))
    } catch {
      case e: java.util.concurrent.TimeoutException =>
        val msg = "Auth Server has not responded to the introspection request within 10 seconds"
        logger.error(msg, e)
        throw AuthException(msg)
    }
  }

}
