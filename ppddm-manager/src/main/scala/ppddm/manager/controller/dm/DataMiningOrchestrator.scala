package ppddm.manager.controller.dm

import akka.Done
import ppddm.core.rest.model.DataMiningModel

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Handles the orchestration of Distributed Data Mining between the Agents.
 *
 * Once all Agents finish AlgorithmExecutionRequests, hence make their fitted models ready, these models need to be sent
 * to the other agents to calculate the statistics.
 *
 */
object DataMiningOrchestrator {

  def startOrchestration(dataMiningModel: DataMiningModel): Future[Done] = {
    Future.apply(Done)
  }

}
