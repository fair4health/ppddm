package ppddm.manager.gateway

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import com.typesafe.scalalogging.Logger
import ppddm.manager.gateway.api.endpoint.ManagerEndpoint

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object ManagerHttpServer extends ManagerEndpoint {
  private val logger: Logger = Logger(this.getClass)

  def start(host: String, port: Int, baseUri: String)(implicit system: ActorSystem): Unit = {
    val f = Http().bindAndHandle(mainRoute(baseUri), host, port)
    try {
      Await.result(f, Duration(10, TimeUnit.SECONDS))
    } catch {
      case e: java.util.concurrent.TimeoutException =>
        logger.error("Starting the Http Server has not finished within 10 seconds", e)
    } finally {
      logger.info("Manager HTTP Server started. Listening on {}:{} with baseUri:{}", host, port, baseUri)
    }
  }
}
