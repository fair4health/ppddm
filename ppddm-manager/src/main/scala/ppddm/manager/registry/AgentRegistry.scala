package ppddm.manager.registry

import com.typesafe.scalalogging.Logger
import ppddm.core.util.JsonFormatter._
import ppddm.core.rest.model.Agent
import ppddm.manager.config.ManagerConfig

import scala.io.Source

object AgentRegistry {

  private val logger: Logger = Logger(this.getClass)

  // TODO Fetch data sources from Service Registry

  val agents: Seq[Agent] =  {
    val fileContent:String = Source.fromResource(ManagerConfig.agentsDefinitionPath).mkString
    val agentList = fileContent.extract[Seq[Agent]]
    logger.info("A total of {} registered agents (data sources) have been retrieved.", agentList.size)
    logger.info("The Manager is starting up with the following Agents: " + agentList.map(_.toPrettyJson))
    agentList
  }

}
