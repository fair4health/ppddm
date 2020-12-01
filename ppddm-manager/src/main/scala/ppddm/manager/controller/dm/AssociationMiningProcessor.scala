package ppddm.manager.controller.dm

import akka.Done
import com.typesafe.scalalogging.Logger
import ppddm.core.rest.model.{DataMiningModel, DataMiningState}
import ppddm.manager.exception.DataIntegrityException

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Processor object for the DataMiningModels whose Projects are of type ProjectType.ASSOCIATION
 */
object AssociationMiningProcessor {

  private val logger: Logger = Logger(this.getClass)

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
      case Some(DataMiningState.CALCULATING_FREQUENCY_ARL) =>
        // This DataMiningModel is still calculating the item frequencies on Agents.
        handleCalculatingFrequencyState(dataMiningModel)
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
    logger.debug(s"The state of DataMiningModel:${dataMiningModel.model_id.get} is None. It is now being processed for the first time by handleNoState.")
    DistributedDataMiningManager.invokeAgentsARLFrequencyCalculation(dataMiningModel) flatMap { _ =>
      // After invoking the frequency calculation endpoints of all Agents, update the state of the DataMiningController in the database
      val newDataMiningModel = dataMiningModel.withDataMiningState(DataMiningState.CALCULATING_FREQUENCY_ARL)
      DataMiningModelController.updateDataMiningModel(newDataMiningModel) map { res =>
        if (res.isEmpty) {
          throw DataIntegrityException(s"data_mining_state of the DataMiningModel cannot be updated after the frequency calculation requests are sent to the Agents. " +
            s"model_id:${dataMiningModel.model_id.get}")
        }
        logger.debug(s"ARL frequency calculation endpoints of the Agents have been invoked and the state for this DataMiningModel:${dataMiningModel.model_id.get} is now CALCULATING_FREQUENCY_ARL.")
        Done
      }
    }
  }

  /**
   * Handle the processing of a DataMiningModel who are in the CALCULATING_FREQUENCY_ARL state.
   *
   * @param dataMiningModel
   * @return
   */
  private def handleCalculatingFrequencyState(dataMiningModel: DataMiningModel): Future[Done] = {
    logger.debug(s"The state of DataMiningModel:${dataMiningModel.model_id.get} is CALCULATING_FREQUENCY_ARL. It is now being processed by handleCalculatingFrequencyState.")
    DistributedDataMiningManager.askAgentsARLFrequencyCalculationResults(dataMiningModel) flatMap { arlFrequencyCalculationResults =>
      null // TODO
    }
  }

}
