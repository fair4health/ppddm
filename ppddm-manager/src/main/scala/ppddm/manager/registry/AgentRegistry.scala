package ppddm.manager.registry

import java.io.FileNotFoundException

import com.typesafe.scalalogging.Logger
import ppddm.core.util.JsonFormatter._
import ppddm.core.rest.model.Agent
import ppddm.manager.config.ManagerConfig

import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * Act as the registry of Agents. It is better to manage the registered Agents with an external service registry, but
 * we implemented it here for practical reasons.
 */
object AgentRegistry {

  private val logger: Logger = Logger(this.getClass)

  // TODO Fetch data sources from Service Registry

  val agents: Seq[Agent] = {
    val fileContent = readFileContent(ManagerConfig.agentsDefinitionPath)
    val agentList = fileContent.extract[Seq[Agent]]
    logger.info("A total of {} registered agents (data sources) have been retrieved.", agentList.size)
    logger.info("The Manager is starting up with the following Agents: " + agentList.map(_.toJson))
    agentList
  }

  private def readFileContent(path: String): String = {
    Try(Source.fromResource(path).mkString) match {
      case Success(fileContent) => fileContent
      case Failure(_) =>
        logger.debug("File:{} cannot be found as a resource, it will be tried to be accessed as an external file")
        val source = Source.fromFile(path)
        try {
          source.mkString
        } catch {
          case e: Exception =>
            val msg = s"Cannot read the Agent definitions from file:${path}"
            logger.error(msg, e)
            throw new FileNotFoundException(msg)
        } finally {
          source.close()
        }
    }
  }

}
