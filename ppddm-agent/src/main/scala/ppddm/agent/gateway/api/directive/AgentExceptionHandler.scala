package ppddm.agent.gateway.api.directive

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.ExceptionHandler
import com.typesafe.scalalogging.Logger
import ppddm.core.exception.AuthException
import ppddm.core.rest.model.DataPreparationException


trait AgentExceptionHandler {

  private val logger: Logger = Logger(this.getClass)

  //handles all the exception during request handling
  val ppddmExceptionHandler: ExceptionHandler = ExceptionHandler {
    case e: AuthException =>
      logger.error(s"AuthException: ${e.getMessage}", e)
      complete(StatusCodes.Unauthorized -> s"Not Authorized. ${e.getMessage}")
    case e:DataPreparationException =>
      logger.error(s"DataPreparationException: ${e.getMessage}", e)
      complete(StatusCodes.BadRequest -> s"DataPreparationException. ${e.getMessage}")
    case e: Exception =>
      logger.error("Unknown Exception", e)
      complete(StatusCodes.InternalServerError -> s"UNKNOWN EXCEPTION: ${e.getMessage}")
  }
}
