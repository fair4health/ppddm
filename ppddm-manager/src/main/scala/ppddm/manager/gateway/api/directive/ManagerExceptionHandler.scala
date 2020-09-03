package ppddm.manager.gateway.api.directive

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.ExceptionHandler
import com.typesafe.scalalogging.Logger
import ppddm.core.exception.{AuthException, DBException}
import ppddm.manager.exception.AgentCommunicationException

trait ManagerExceptionHandler {
  private val logger: Logger = Logger(this.getClass)

  //handles all the exception during request handling
  val ppddmExceptionHandler: ExceptionHandler = ExceptionHandler {
    case e: AuthException =>
      logger.error(s"AuthException: ${e.getMessage}", e)
      complete(StatusCodes.Unauthorized -> s"Not Authorized. ${e.getMessage}")
    case e: DBException =>
      logger.error(s"DBException: ${e.getMessage}", e)
      complete(StatusCodes.InternalServerError -> s"Error with the MongoDB Database. ${e.getMessage}")
    case e: AgentCommunicationException =>
      val msg = s"AgentCommunicationException while connecting to Agent on URI: ${e.url} whose name:${e.name}: ${e.getMessage}"
      logger.error(msg, e)
      complete(StatusCodes.InternalServerError -> msg)
    case e: Exception =>
      logger.error("Unknown Exception", e)
      complete(StatusCodes.InternalServerError -> s"UNKNOWN EXCEPTION: ${e.getMessage}")
  }
}
