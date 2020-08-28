package ppddm.manager.registry

import com.typesafe.scalalogging.Logger
import ppddm.core.util.JsonFormatter._
import ppddm.core.rest.model.DataSource

import scala.io.Source

object AgentRegistry {

  private val logger: Logger = Logger(this.getClass)

  val dataSources: Seq[DataSource] =  {
    val fileContent:String = Source.fromResource("datasource.json").mkString
    fileContent.extract[Seq[DataSource]]
  }

}
