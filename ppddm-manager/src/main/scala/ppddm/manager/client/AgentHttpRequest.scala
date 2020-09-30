package ppddm.manager.client

import akka.http.scaladsl.model.HttpRequest
import ppddm.core.rest.model.Agent

final case class AgentHttpRequest (agent: Agent, httpRequest: HttpRequest)
