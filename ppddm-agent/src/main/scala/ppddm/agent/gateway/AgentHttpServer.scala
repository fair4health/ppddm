package ppddm.agent.gateway

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import com.typesafe.scalalogging.Logger
import ppddm.agent.gateway.api.endpoint.AgentEndpoint

/**
 * The Http server of the PPDDM-Agent which serves through the #AgentEndpoint which includes the main route
 * for the REST services.
 */
object AgentHttpServer extends AgentEndpoint {

  private val logger: Logger = Logger(this.getClass)

  def start(host: String, port: Int, baseUri: String)(implicit system: ActorSystem): Unit = {
    Http().bindAndHandle(mainRoute(baseUri), host, port)
    logger.info("Agent HTTP Server started. Listening on {}:{} with baseUri:{}", host, port, baseUri)
  }

}
