package ppddm.manager.gateway

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import com.typesafe.scalalogging.Logger
import ppddm.manager.gateway.api.endpoint.ManagerEndpoint

object ManagerHttpServer extends ManagerEndpoint {
  private val logger: Logger = Logger(this.getClass)

  def start(host: String, port: Int, baseUri: String)(implicit system: ActorSystem): Unit = {
    Http().bindAndHandle(mainRoute(baseUri), host, port)
    logger.info("Agent HTTP Server started. Listening on {}:{} with baseUri:{}", host, port, baseUri)
  }
}
