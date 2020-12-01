package ppddm.manager.controller.dm

import java.util.concurrent.TimeUnit

import akka.Done
import com.typesafe.scalalogging.Logger
import ppddm.core.rest.model.{ARLFrequencyCalculationResult, BoostedModel, DataMiningModel, DataMiningState}
import ppddm.manager.exception.DataIntegrityException

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

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
      case Some(DataMiningState.EXECUTING_ARL) =>
        // This DataMiningModel is still executing the ARL on Agents.
        handleExecutingARLState(dataMiningModel)
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
      // results include the ARLFrequencyCalculationResult of the Agents whose frequency calculation is completed.
      // Others have not finished yet.

      // A ARLFrequencyCalculationResult includes a sequence of WeakModel (one for each Algorithm)
      // All ModelTrainingResults should include WeakModels of the same set of Algorithms

      // TODO 1. Create a BoostedModel (if not created already) and add the WeakModels created out of the returned ARLFrequencyCalculationResult
      // TODO 2. If there are no remaining Agents to wait for the frequency calculation results
      // TODO 2.1. We will merge the results, find the set of the frequent items above the given support threshold
      // TODO 2.2. We can call the arl execution endpoints of the Agents with the decided frequent item set and advance to the EXECUTING_ARL state
      // TODO 3. Update the DataMiningModel in the database

      null
    }
  }

  /**
   * Handle the processing of a DataMiningModel who are in the EXECUTING_ARL state.
   *
   * @param dataMiningModel
   * @return
   */
  private def handleExecutingARLState(dataMiningModel: DataMiningModel): Future[Done] = {

    // TODO 1. Update the WeakModels with the received fitted_models
    // TODO 2. If there are no remaining AGents to wait for ARL execution results
    // TODO 2.1. Extract the statistics (rules) from the fitted_models and combine them (find a new data structure)
    // TODO 2.2. Update the BoostedModel with this final combined statistics and finish the scheduled processing
    // TODO 3. Update the DataMiningModel in the database

    null
  }

}
