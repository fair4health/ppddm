package ppddm.agent.controller.service

import ppddm.agent.config.AgentConfig
import ppddm.core.rest.model.Agent

object ServiceController {

  def getMetadata: Agent = {
    Agent(AgentConfig.agentID, AgentConfig.appName, s"${AgentConfig.serverHost}:${AgentConfig.serverPort}:${AgentConfig.baseUri}")
  }

}
