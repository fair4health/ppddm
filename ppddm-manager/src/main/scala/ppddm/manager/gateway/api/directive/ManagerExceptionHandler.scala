package ppddm.manager.gateway.api.directive

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.ExceptionHandler
import com.typesafe.scalalogging.Logger
import ppddm.core.exception.{AuthException, DBException}
import ppddm.manager.exception.{AgentCommunicationException, DataIntegrityException}

import java.io.{PrintWriter, StringWriter}

trait ManagerExceptionHandler {
  private val logger: Logger = Logger(this.getClass)

  //handles all the exception during request handling
  val ppddmExceptionHandler: ExceptionHandler = ExceptionHandler {
    case e: AuthException =>
      logger.error(s"AuthException: ${e.getMessage}", e)
      complete(StatusCodes.Unauthorized -> s"Not Authorized. ${e.getMessage}.\n ${stackTraceToString(e)}")
    case e: DBException =>
      logger.error(s"DBException: ${e.getMessage}", e)
      complete(StatusCodes.InternalServerError -> s"Error with the MongoDB Database. ${e.getMessage}.\n ${stackTraceToString(e)}")
    case e: DataIntegrityException =>
      logger.error(s"DataIntegrityException: ${e.getMessage}", e)
      complete(StatusCodes.InternalServerError -> s"Data integrity error. ${e.getMessage}.\n ${stackTraceToString(e)}")
    case e: AgentCommunicationException =>
      val msg = s"AgentCommunicationException while connecting to Agent on URI: ${e.url} whose name:${e.name}: ${e.getMessage}"
      logger.error(msg, e)
      complete(StatusCodes.InternalServerError -> msg)
    case e: Exception =>
      logger.error("Unknown Exception", e)
      complete(StatusCodes.InternalServerError -> s"UNKNOWN EXCEPTION: ${e.getMessage}.\n ${stackTraceToString(e)}")
  }

  private def stackTraceToString(e: Exception): String = {
    val sw = new StringWriter
    e.printStackTrace(new PrintWriter(sw))
    sw.toString
  }
}
