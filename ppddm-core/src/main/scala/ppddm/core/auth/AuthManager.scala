package ppddm.core.auth

import akka.actor.ActorSystem
import akka.http.caching.LfuCache
import akka.http.caching.scaladsl.Cache
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.Accept
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.typesafe.scalalogging.Logger
import ppddm.core.exception.AuthException
import ppddm.core.rest.model.Json4sSupport._

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, TimeoutException}

/**
 * This object manages the interaction with the authentication server and performs introspection queries
 * to check the validity of the given access tokens to the endpoints. This object maintains a cache so that
 * it does not go to the introspection server for each interaction and checks the validity of the JWT from the cache.
 */
object AuthManager {

  private val logger: Logger = Logger(this.getClass)

  private implicit var system: ActorSystem = _

  private var authContextCache: Cache[String, AuthContext] = _

  private var loginURL: String = _
  private var introspectionURL: String = _
  private var username: String = _
  private var password: String = _

  private var loginResponse: Option[LoginResponse] = Option.empty[LoginResponse]

  // Default headers
  private val defaultHeaders = List(Accept(MediaTypes.`application/json`))

  def init(loginURL: String, introspectionURL: String, username: String, password: String)(implicit system: ActorSystem): Unit = {
    this.system = system
    this.loginURL = loginURL
    this.introspectionURL = introspectionURL
    this.username = username
    this.password = password

    this.authContextCache = LfuCache[String, AuthContext]

    try {
      Await.result(loadAccessToken(), Duration(1, TimeUnit.MINUTES))
    } catch {
      case e: TimeoutException =>
        val msg = s"Access token cannot be retrieved from the auth server at ${this.loginURL}"
        logger.error(msg, e)
        throw AuthException(msg, e)
    }

    logger.info("AuthManager has been initialized successfully.")
  }

  def getOrLoad(key: String, loadValue: String ⇒ Future[AuthContext]): Future[AuthContext] = {
    this.authContextCache.getOrLoad(key, loadValue)
  }

  def get(key: String): Option[Future[AuthContext]] = {
    this.authContextCache.get(key)
  }

  def remove(key: String): Unit = {
    this.authContextCache.remove(key)
  }

  /**
   * This method logins to the auth server and collects the authentication token
   * so that the token can be used for the subsequent interactions with the auth server,
   * such as for the introspection requests.
   */
  def loadAccessToken(): Future[Unit] = {
    logger.debug(s"Logging into ${this.loginURL} to authenticate ${this.username} and gain an accessToken to use in the " +
      s"subsequent calls (i.e. introspection)")
    /* To use the toJson, toPrettyJson methods of the JsonFormatter */
    val httpRequest = HttpRequest(
      uri = Uri(loginURL),
      method = HttpMethods.POST,
      headers = defaultHeaders,
      entity = HttpEntity(ContentTypes.`application/json`, LoginRequest(username, password).toJson))

    Http().singleRequest(httpRequest) flatMap { response =>
      response.status match {
        case StatusCodes.OK =>
          logger.debug(s"Access token for ${this.username} has been successfully retrieved from ${this.loginURL}")
          Unmarshal(response.entity)
            .to[LoginResponse]
            .recover {
              case e: Exception =>
                val msg = s"Error while processing the response of the login attempt to the authentication server."
                logger.error(msg, e)
                throw AuthException(msg, e)
            }
        case StatusCodes.Unauthorized =>
          val msg = "Username or password is wrong for loading the access token from the auth server! " +
            s"username:${username} password:${password}"
          logger.error(msg)
          Unmarshal(response.entity).to[AuthServerException] map { ex =>
            throw AuthException(s"Auth server returned error. Status:${ex.status} Error:${ex.error} Message:${ex.message}")
          }
        case status if response.entity.contentType == ContentTypes.`application/json` =>
          Unmarshal(response.entity).to[AuthServerException] map { ex =>
            throw AuthException(s"Auth server returned error. Status:${ex.status} Error:${ex.error} Message:${ex.message}")
          }
        case status =>
          Unmarshal(response.entity).to[String] map { err =>
            throw AuthException(s"Login attempt to authentication server has failed. " +
              s"The response code is ${status}. The error message is $err")
          }
      }
    } map { loginResponse =>
      this.loginResponse = Some(loginResponse)
    }
  }

  def introspect(userAccessToken: String): Future[AuthContext] = {
    if (this.loginResponse.isEmpty) {
      val msg = s"There is no loginResponse (an accessToken) belonging to the ${this.username} to be used for the introspection requests."
      logger.error(msg)
      throw AuthException(msg)
    }

    logger.debug(s"Introspecting for the user's accessToken:${userAccessToken} at the endpoint: ${this.introspectionURL}")
    /* To use the toJson, toPrettyJson methods of the JsonFormatter */
    import ppddm.core.util.JsonFormatter._
    val httpRequest = HttpRequest(
      uri = Uri(s"${introspectionURL}?token=${this.loginResponse.get.access_token}"),
      method = HttpMethods.POST,
      headers = defaultHeaders)

    Http().singleRequest(httpRequest) flatMap { response =>
      response.status match {
        case StatusCodes.OK =>
          Unmarshal(response.entity)
            .to[Boolean]
            .recover {
              case e: Exception =>
                val msg = s"Error while processing the response of the login attempt to the authentication server."
                logger.error(msg, e)
                throw AuthException(msg, e)
            } map { introspectionResponse =>
            if (introspectionResponse) {
              // Unfortunately ATOS's auth server returns true or false within a HTTP OK response to the request that we make above
              new AuthContext(userAccessToken)
            } else {
              throw AuthException(s"This user's accessToken cannot be validated: ${userAccessToken}")
            }
          }
        case StatusCodes.Unauthorized =>
          // AccessToken has expired, reload it from auth server and call this function recursively
          logger.info("Access Token not authorized. Will re-authenticate on the auth server and make the introspection request again...")
          response.discardEntityBytes()
          loadAccessToken() flatMap { _ =>
            introspect(userAccessToken)
          }
        case status =>
          Unmarshal(response.entity).to[String] map { err =>
            throw AuthException(s"Introspection attempt to authentication server has failed for the user access token: ${userAccessToken}. " +
              s"The response code is ${status}. The error message is $err")
          }
      }
    }
  }

}
