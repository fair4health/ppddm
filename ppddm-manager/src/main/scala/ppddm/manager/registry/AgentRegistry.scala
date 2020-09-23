package ppddm.manager.registry

import com.typesafe.scalalogging.Logger
import ppddm.core.util.JsonFormatter._
import ppddm.core.rest.model.Agent

import scala.io.Source

object AgentRegistry {

  private val logger: Logger = Logger(this.getClass)

  // TODO Fetch data sources from Service Registry

  val agents: Seq[Agent] =  {
    val fileContent:String = Source.fromResource("agents.json").mkString
    val agentList = fileContent.extract[Seq[Agent]]
    logger.debug("A total of {} registered agents (data sources) have been retrieved.", agentList.size)
    agentList
  }

}
