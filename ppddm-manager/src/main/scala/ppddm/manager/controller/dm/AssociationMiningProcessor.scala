package ppddm.manager.controller.dm

import akka.Done
import ppddm.core.rest.model.DataMiningModel

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object AssociationMiningProcessor {

  /**
   * Handle the processing of the DataMiningModel of ProjectType.ASSOCIATION type Projects
   *
   * @param dataMiningModel
   * @return
   */
  def processAssociationMining(dataMiningModel: DataMiningModel): Future[Done] = {
    Future{ Done }
  }

}
