package ppddm.manager.registry

import com.typesafe.scalalogging.Logger
import ppddm.core.util.JsonFormatter._
import ppddm.core.rest.model.DataSource

import scala.io.Source

object AgentRegistry {

  private val logger: Logger = Logger(this.getClass)

  // TODO Fetch data sources from Service Registry

  val dataSources: Seq[DataSource] =  {
    val fileContent:String = Source.fromResource("datasource.json").mkString
    val dsList = fileContent.extract[Seq[DataSource]]
    logger.debug("A total of {} registered data sources (agents) have been retrieved.", dsList.size)
    dsList
  }

}
