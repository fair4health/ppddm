package ppddm.agent.gateway.api.directive

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, ExceptionHandler, Route}
import com.typesafe.scalalogging.Logger
import ppddm.core.exception.AuthException


trait AgentExceptionHandler {

  private val logger: Logger = Logger(this.getClass)

  //handles all the exception during request handling
  val ppddmExceptionHandler: ExceptionHandler = ExceptionHandler {
    case e: AuthException =>
      logger.error(s"AuthException: ${e.getMessage}", e)
      complete(StatusCodes.Unauthorized -> s"Not Authorized. ${e.getMessage}")
    case e: Exception =>
      logger.error("Unknown Exception", e)
      complete(StatusCodes.InternalServerError -> s"UNKNOWN EXCEPTION: ${e.getMessage}")
  }
}
