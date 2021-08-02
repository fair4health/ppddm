package ppddm.manager.registry

import akka.http.scaladsl.model.HttpMethods
import com.typesafe.scalalogging.Logger
import ppddm.core.rest.model.Agent
import ppddm.manager.client.AgentClient

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

/**
 * Check the health status of the given agents.
 */
object AgentHealthChecker {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Check the health status of the given Agent. If the Option inside the Future is defined,
   * then the Agent is healthy.
   *
   * @param agent
   * @return
   */
  private def checkAgentHealth(agent: Agent): Future[Option[Agent]] = {
    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.GET, agent.getMetadataURI)
    AgentClient.invokeHttpRequest[Agent](agentRequest).map(_.toOption)
  }

  /**
   * This is a blocking call to check whether the registered agents are healthy or not.
   * @param agents
   */
  def checkAllAgentsHealth(): Unit = {
    import ppddm.core.util.JsonFormatter._

    logger.info("******** Agents Health Check ********")

    val agentsHealthStatus: Seq[(Agent, Boolean)] = AgentRegistry.agents.map { agent =>
      val response = Try(Await.result(checkAgentHealth(agent), Duration(5, TimeUnit.SECONDS))).toOption
      if(response.isEmpty) {
        logger.error(s"There is a non-responsive/unhealthy agent. ${agent.toJson}")
        (agent, false)
      } else {
        (agent, true)
      }
    }
    if(agentsHealthStatus.exists(!_._2)) {
      // There is at least 1 unhealthy agent
      val healthyAgents = agentsHealthStatus.filter(_._2 == true)
      logger.info(s"There are ${agentsHealthStatus.size} registered agents, but ${healthyAgents.size} of them are healthy.")
      logger.info(s"The healthy agents are: ${healthyAgents.map {case (a, _) => a.toJson}}")
    } else {
      logger.info(s"There are ${agentsHealthStatus.size} registered agents and ALL are healthy.")
    }

    logger.info("//////////// Agents Health Check ////////////")
  }

}
