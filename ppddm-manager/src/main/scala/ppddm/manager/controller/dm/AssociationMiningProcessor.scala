package ppddm.manager.controller.dm

import akka.Done
import ppddm.core.rest.model.{DataMiningModel, DataMiningState}
import ppddm.manager.exception.DataIntegrityException

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
    dataMiningModel.data_mining_state match {
      case None =>
        // This is the first time of this DataMiningModel with us ;) Start the frequency calculation
        handleNoState(dataMiningModel)
//      case Some(DataMiningState.CALCULATING_FREQUENCY_ARL) =>
//        // This DataMiningModel is still training the fitted_models on Agents.
//        handleTrainingState(dataMiningModel)
//      case Some(DataMiningState.EXECUTING_ARL) =>
//        // This DataMiningModel's fitted_models are being validated on the other Agents. Ask to the Agents whether they are completed or not.
//        // If they are all completed, finalize the WeakModels, create the BoostedModel to be tested
//        handleValidationState(dataMiningModel)
      case Some(DataMiningState.READY) | Some(DataMiningState.FINAL) =>
        // This is already in its READY or FINAL state, this block should not execute in normal circumstances.
        Future.apply(DataMiningOrchestrator.stopOrchestration(dataMiningModel.model_id.get)) // Stop the orchestration for this DataMiningModel
        val msg = s"This DataMiningModel:${dataMiningModel.model_id.get} is already in its ${dataMiningModel.data_mining_state.get} state, why do you want me to process it within an Orchestrator!!!"
        throw DataIntegrityException(msg)
      case Some(state) =>
        val msg = s"Hey..! There is no such state:${state} for the DataMiningObject for Association projects."
        throw new IllegalArgumentException(msg)
    }
  }

  /**
   * Handle the processing of a DataMiningModel who has not assigned a DataMiningState.
   * This dataMiningModel is just starting to get processed.
   *
   * @param dataMiningModel
   * @return
   */
  private def handleNoState(dataMiningModel: DataMiningModel): Future[Done] = {
    Future { Done }
  }

}
