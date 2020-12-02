package ppddm.manager.controller.dm

import akka.Done
import com.typesafe.scalalogging.Logger
import ppddm.core.rest.model.{BoostedModel, DataMiningModel, DataMiningState, WeakModel}
import ppddm.manager.exception.DataIntegrityException

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

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
        handleCalculatingFrequencyARLState(dataMiningModel)
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
  private def handleCalculatingFrequencyARLState(dataMiningModel: DataMiningModel): Future[Done] = {
    logger.debug(s"The state of DataMiningModel:${dataMiningModel.model_id.get} is CALCULATING_FREQUENCY_ARL. It is now being processed by handleCalculatingFrequencyARLState.")
    DistributedDataMiningManager.askAgentsARLFrequencyCalculationResults(dataMiningModel) flatMap { arlFrequencyCalculationResults =>
      // results include the ARLFrequencyCalculationResult of the Agents whose frequency calculation is completed.
      // Others have not finished yet.

      // A ARLFrequencyCalculationResult includes a sequence of WeakModel (one for each Algorithm)
      // However, in our settings now, we only support one algorithm (FP-Growth) and only one WeakModel will be returned by each Agent.

      val newBoostedModels = dataMiningModel.algorithms.map { algorithm => // For each algorithm of this dataMiningModel (we know that there will only be 1 algorithms, but to be compliant with the PredictionMiningProcessor loop over it

        val weakModelsOfAlgorithm = arlFrequencyCalculationResults.map { arlFrequencyCalculationResult =>
          WeakModel(
            algorithm = algorithm,
            agent = arlFrequencyCalculationResult.agent,
            fitted_model = None,
            item_frequencies = Some(arlFrequencyCalculationResult.item_frequencies),
            total_record_count = Some(arlFrequencyCalculationResult.total_record_count),
            training_statistics = None,
            validation_statistics = None,
            calculated_statistics = None,
            weight = None
          )
        }

        // Check whether this DataMiningModel has already a BoostedModel for this algorithm (look up, we are looping over the algorithms)
        val existingBoostedModel = if (dataMiningModel.boosted_models.isDefined) dataMiningModel.boosted_models.get.find(_.algorithm.name == algorithm.name) else None

        if (existingBoostedModel.isDefined) {
          // This means we previously created a BoostedModel for this algorithm and saved into the database.
          // Now, new WeakModels arrived from different Agents for this BoostedModel (look up, we are looping over the algorithms)
          logger.debug("There is already a BoostedModel for this Algorithm:{} under the DataMiningModel with model_id:{}. " +
            "Newcoming WeakModels will be added to that BoostedModel.", algorithm.name, dataMiningModel.model_id.get)
          existingBoostedModel.get.addNewWeakModels(weakModelsOfAlgorithm)
        } else {
          // This means this is the first time that we will create a BoostedModel for this algorithm
          logger.debug("Creating a BoostedModel for the 1st time for the Algorithm:{} under the DataMiningModel with model_id:{}",
            algorithm.name, dataMiningModel.model_id.get)
          BoostedModel(algorithm, weakModelsOfAlgorithm, None, None, None, None) // Create the BoostedModel for this algorithm for the first time
        }
      }

      var newDataMiningModel = dataMiningModel.withBoostedModels(newBoostedModels)

      if (DataMiningModelController.getAgentsWaitedForARLFrequencyCalculationResults(newDataMiningModel).isEmpty) {
        // If there are no remaining Agents to wait for the ARL frequency calculation results,
        // then we will combine the frequency results, find the set of the frequent items above the given support threshold
        // and update the BoostedModel
        logger.debug("There are no remaining Agents being waited for frequency calculation results. So, I will calculate " +
          "the combined item frequencies and then invoke the ARL execution endpoints of the Agents and then " +
          s"update the state to EXECUTING_ARL for this DataMiningModel:${dataMiningModel.model_id.get}")

        // TODO: Call the combine function like we do in the TESTING state of the PredictionMiningProcessor and then update the model

        val f = DistributedDataMiningManager.invokeAgentsARLExecution(newDataMiningModel)
        try { // Wait for the ARL execution invocations finish for all Agents
          Await.result(f, Duration(30, TimeUnit.SECONDS))
        } catch {
          case e: java.util.concurrent.TimeoutException =>
            logger.error("Invoking the ARK execution endpoints of {} Agents have not finished within 30 seconds " +
              "for DataMiningModel with model_id: {}.", DataMiningModelController.getSelectedAgents(dataMiningModel).length, dataMiningModel.model_id.get, e)
        }
        logger.debug(s"ARL execution endpoints of the Agents have been invoked and the state for this DataMiningModel:${dataMiningModel.model_id.get} will be advanced to EXECUTING_ARL.")

        // Advance the state to EXECUTING_ARL
        newDataMiningModel = newDataMiningModel.withDataMiningState(DataMiningState.EXECUTING_ARL)
      } else {
        logger.debug(s"There are still remaining Agents being waited for ARL frequency calculation results for this DataMiningModel:${dataMiningModel.model_id.get}")
      }

      // After processing the received ARLFrequencyCalculationResult, save the newDataMiningModel into the database
      DataMiningModelController.updateDataMiningModel(newDataMiningModel) map { res =>
        if (res.isEmpty) {
          throw DataIntegrityException(s"data_mining_state of the DataMiningModel cannot be updated after the ARL frequency calculation results are received from the Agents. " +
            s"model_id:${newDataMiningModel.model_id.get}")
        }
        logger.debug(s"handleCalculatingFrequencyARLState finished for this DataMiningModel:${dataMiningModel.model_id.get} Its state is ${newDataMiningModel.data_mining_state.get}.")
        Done
      }
    }
  }

  /**
   * Handle the processing of a DataMiningModel who are in the EXECUTING_ARL state.
   *
   * @param dataMiningModel
   * @return
   */
  private def handleExecutingARLState(dataMiningModel: DataMiningModel): Future[Done] = {
    logger.debug(s"The state of DataMiningModel:${dataMiningModel.model_id.get} is EXECUTING_ARL. It is now being processed by handleExecutingARLState.")
    DistributedDataMiningManager.askAgentsARLExecutionResults(dataMiningModel) flatMap { arlExecutionResults =>
      // ARLExecutionResults of the Agents whose ARL execution is completed.
      // Others have not finished yet.

      // An ARLExecutionResults includes a sequence of ARLModels (one for each Algorithm)

      val updatedBoostedModels = dataMiningModel.algorithms.map { algorithm => // For each algorithm of this dataMiningModel (we know that there will only be 1 algorithms, but to be compliant with the PredictionMiningProcessor loop over it

        // Let's do some integrity checks
        if (dataMiningModel.boosted_models.isEmpty) {
          throw DataIntegrityException(s"There are no BoostedModels for this DataMiningModel:${dataMiningModel.model_id.get}." +
            s"I am trying to the process handleExecutingARLState and there must have been a BoostedModel for each Algorithm.")
        }

        val boostedModelOption = dataMiningModel.boosted_models.get.find(_.algorithm.name == algorithm.name)
        if (boostedModelOption.isEmpty) {
          throw DataIntegrityException(s"Sweet Jesus! I am processing the handleExecutingARLState of this DataMiningModel:${dataMiningModel.model_id.get}, " +
            s"however there is no corresponding BoostedModel for this Algorithm:${algorithm.name}")
        }

        val boostedModel = boostedModelOption.get

        val updatedWeakModels = boostedModel.weak_models.map { weakModel =>
          val arlExecutionResultOption = arlExecutionResults.find(_.agent.agent_id == weakModel.agent.agent_id)
          if(arlExecutionResultOption.isDefined) {
            val arlExecutionResult = arlExecutionResultOption.get
            // The Agent of this WeakModel returned the ARL execution result
            val arlModelOption = arlExecutionResult.arl_models.find(_.algorithm.name == algorithm.name)

            if (arlModelOption.isEmpty) {
              // If there is a result from an Agent, it must contain an ARLModel for each Algorithm of this DataMiningModel, because we submitted it previously for ARL execution
              throw DataIntegrityException(s"The Algorithm with name ${algorithm.name} could not be found in the ARLExecutionResult " +
                s" of ${arlExecutionResult.agent} for the DataMiningModel with model_id:${dataMiningModel.model_id} and name:${dataMiningModel.name}")
            }
            val arlModel = arlModelOption.get
            if(arlModel.agent.agent_id != weakModel.agent.agent_id) {
              throw DataIntegrityException(s"The agent of the ARLModel is different than the agent of the WeakModel it is being assigned to! This means " +
                s"the agent of the ARLModel is also different than the agent of the encapsulating ARLExecutionResult. This cannot happen!!!")
            }
            weakModel.withFittedModel(arlModel.fitted_model)
          } else {
            // The Agent of this WeakModel has not finished the ARL execution yet
            weakModel
          }
        }
        boostedModel.replaceWeakModels(updatedWeakModels)
      }

      var newDataMiningModel = dataMiningModel.withBoostedModels(updatedBoostedModels)

      if (DataMiningModelController.getAgentsWaitedForARLExecutionResults(newDataMiningModel).isEmpty) {
        logger.debug("There are no remaining Agents being waited for ARL execution results. So, I will calculate the " +
          s"association rules (statistics) for the BoostedModels of this DataMiningModel:${dataMiningModel.model_id.get}")

        // TODO 2.1. Extract the statistics (rules) from the fitted_models and combine them (find a new data structure)
        // TODO 2.2. Update the BoostedModel with this final combined statistics and finish the scheduled processing

        // Advance the state to READY
        newDataMiningModel = newDataMiningModel.withDataMiningState(DataMiningState.READY)
      } else {
        logger.debug(s"There are still remaining Agents being waited for ARL execution results of this DataMiningModel:${dataMiningModel.model_id.get}")
      }

      DataMiningModelController.updateDataMiningModel(newDataMiningModel) map { res =>
        if (res.isEmpty) {
          throw DataIntegrityException(s"data_mining_state of the DataMiningModel cannot be updated after the model test results are received from the Agents. " +
            s"model_id:${newDataMiningModel.model_id.get}")
        }

        if (newDataMiningModel.data_mining_state.get == DataMiningState.READY) {
          // Stop the orchestration for this DataMiningModel is a separate thread, only if its state is READY
          Future.apply(DataMiningOrchestrator.stopOrchestration(newDataMiningModel.model_id.get))
        }

        logger.debug(s"handleExecutingARLState finished for this DataMiningModel:${dataMiningModel.model_id.get} Its state is ${newDataMiningModel.data_mining_state.get}.")

        Done
      }
    }
  }

}
