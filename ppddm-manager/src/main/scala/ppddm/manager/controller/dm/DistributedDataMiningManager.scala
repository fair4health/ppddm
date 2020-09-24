package ppddm.manager.controller.dm

import akka.Done
import com.typesafe.scalalogging.Logger
import ppddm.core.rest.model.DataMiningModel

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * This object handles the interaction with the Agents/Datasources so that the fedarated data mining algorithms can be
 * executed on each Datasource endpoint and this execution is orchestrated.
 */
object DistributedDataMiningManager {

  private val logger: Logger = Logger(this.getClass)

  /**
   *
   *
   * @param dataMiningModel
   * @return
   */
  def startOrchestrationForDistributedDataMining(dataMiningModel: DataMiningModel): Future[Done] = {
    dataMiningModel.dataset.dataset_sources.get.foreach { datasetSource =>
      //datasetSource.agent.getDataMiningURI()
    }
    Future.apply(Done)
  }

}
