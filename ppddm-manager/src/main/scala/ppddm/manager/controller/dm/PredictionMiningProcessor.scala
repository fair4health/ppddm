package ppddm.manager.controller.dm

import java.util.concurrent.TimeUnit

import akka.Done
import com.typesafe.scalalogging.Logger
import ppddm.core.ai.{Aggregator, StatisticsCalculator}
import ppddm.core.rest.model.{BoostedModel, DataMiningModel, DataMiningState}
import ppddm.manager.exception.DataIntegrityException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object PredictionMiningProcessor {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Handle the processing of the DataMiningModel of ProjectType.PREDICTION type Projects
   *
   * @param dataMiningModel
   * @return
   */
  def processPredictionMining(dataMiningModel: DataMiningModel): Future[Done] = {
    dataMiningModel.data_mining_state match {
      case None =>
        // This is the first time of this DataMiningModel with us ;) Start the training
        handleNoState(dataMiningModel)
      case Some(DataMiningState.TRAINING) =>
        // This DataMiningModel is still training the fitted_models on Agents.
        handleTrainingState(dataMiningModel)
      case Some(DataMiningState.VALIDATING) =>
        // This DataMiningModel's fitted_models are being validated on the other Agents. Ask to the Agents whether they are completed or not.
        // If they are all completed, finalize the WeakModels, create the BoostedModel to be tested
        handleValidationState(dataMiningModel)
      case Some(DataMiningState.TESTING) =>
        // This DataMiningModel has a BoostedModel which is being tested on the Agents. Ask to the Agents whether they are completed or not.
        // If they are all completed, we can finalize the DataMiningModel
        // After this block, the scheduled process should be removed/cancelled
        handleTestingState(dataMiningModel)
      case Some(DataMiningState.READY) | Some(DataMiningState.FINAL) =>
        // This is already in its READY or FINAL state, this block should not execute in normal circumstances.
        Future.apply(DataMiningOrchestrator.stopOrchestration(dataMiningModel.model_id.get)) // Stop the orchestration for this DataMiningModel
        val msg = s"This DataMiningModel:${dataMiningModel.model_id.get} is already in its ${dataMiningModel.data_mining_state.get} state, why do you want me to process it within an Orchestrator!!!"
        throw DataIntegrityException(msg)
      case Some(state) =>
        val msg = s"Hey..! There is no such state:${state} for the DataMiningObject for Prediction projects."
        throw new IllegalArgumentException(msg)
    }
  }

  /**
   * Handle the processing of a DataMiningModel who has not assigned DataMiningState.
   * This dataMiningModel is just starting to get processed.
   *
   * @param dataMiningModel
   * @return
   */
  private def handleNoState(dataMiningModel: DataMiningModel): Future[Done] = {
    logger.debug(s"The state of DataMiningModel:${dataMiningModel.model_id.get} is None. It is now being processed for the first time by handleNoState.")
    DistributedDataMiningManager.invokeAgentsModelTraining(dataMiningModel) flatMap { _ =>
      // After invoking the training endpoints of all Agents, update the state of the DataMiningController in the database
      val newDataMiningModel = dataMiningModel.withDataMiningState(DataMiningState.TRAINING)
      DataMiningModelController.updateDataMiningModel(newDataMiningModel) map { res =>
        if (res.isEmpty) {
          throw DataIntegrityException(s"data_mining_state of the DataMiningModel cannot be updated after the model training requests are sent to the Agents. " +
            s"model_id:${dataMiningModel.model_id.get}")
        }
        logger.debug(s"Model training endpoints of the Agents have been invoked and the state for this DataMiningModel:${dataMiningModel.model_id.get} is now TRAINING.")
        Done
      }
    }
  }

  /**
   * Handle the processing of a DataMiningModel whose is in DataMiningState.TRAINING state.
   *
   * @param dataMiningModel
   * @return
   */
  private def handleTrainingState(dataMiningModel: DataMiningModel): Future[Done] = {
    logger.debug(s"The state of DataMiningModel:${dataMiningModel.model_id.get} is TRAINING. It is now being processed by handleTrainingState.")
    DistributedDataMiningManager.askAgentsModelTrainingResults(dataMiningModel) flatMap { modelTrainingResults =>
      // results include the ModelTrainingResults of the Agents whose training is completed.
      // Others have not finished yet.

      // A ModelTrainingResult includes a sequence of WeakModel (one for each Algorithm)
      // All ModelTrainingResults should include WeakModels of the same set of Algorithms

      val newBoostedModels = dataMiningModel.algorithms.map { algorithm => // For each algorithm of this dataMiningModel

        val weakModelsOfAlgorithm = modelTrainingResults.map { modelTrainingResult => // find the corresponding WeakModel returned by each Agent and make a sequence of it
          val wm = modelTrainingResult.algorithm_training_models.find(_.algorithm.name == algorithm.name)
          if (wm.isEmpty) {
            // If there is a result from an Agent, it must contain a WeakModel for each Algorithm of this DataMiningModel, because we submitted it previously for model training
            throw DataIntegrityException(s"The Algorithm with name ${algorithm.name} could not be found on the WeakModel " +
              s"training results of ${modelTrainingResult.agent} for the DataMiningModel with model_id:${dataMiningModel.model_id} and name:${dataMiningModel.name}")
          }
          wm.get
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
          BoostedModel(algorithm, weakModelsOfAlgorithm, None, None, None) // Create the BoostedModel for this algorithm for the first time
        }

      }

      var newDataMiningModel = dataMiningModel.withBoostedModels(newBoostedModels)

      if (DataMiningModelController.getAgentsWaitedForTrainingResults(newDataMiningModel).isEmpty) {
        // If there are no remaining Agents to wait for the training results,
        // then we can call the model validation endpoints of the Agents and advance to the VALIDATING state
        logger.debug("There are no remaining Agents being waited for training results. So, I will invoke the validation endpoints of the Agents and " +
          s"update the state to VALIDATING for this DataMiningModel:${dataMiningModel.model_id.get}")
        val f = DistributedDataMiningManager.invokeAgentsModelValidation(newDataMiningModel)
        try { // Wait for the validate invocations finish for all Agents
          Await.result(f, Duration(30, TimeUnit.SECONDS))
        } catch {
          case e: java.util.concurrent.TimeoutException =>
            logger.error("Invoking the model validation endpoints of {} Agents have not finished within 30 seconds " +
              "for DataMiningModel with model_id: {}.", DataMiningModelController.getSelectedAgents(dataMiningModel).length, dataMiningModel.model_id.get, e)
        }

        logger.debug(s"Model validation endpoints of the Agents have been invoked and the state for this DataMiningModel:${dataMiningModel.model_id.get} will be advanced to VALIDATING.")
        // Advance the state to VALIDATING because the Agents started validating the WeakModels
        newDataMiningModel = newDataMiningModel.withDataMiningState(DataMiningState.VALIDATING)
      } else {
        logger.debug(s"There are still remaining Agents being waited for training results for this DataMiningModel:${dataMiningModel.model_id.get}")
      }

      // After processing the received ModelTrainingResult, save the newDataMiningModel into the database
      DataMiningModelController.updateDataMiningModel(newDataMiningModel) map { res =>
        if (res.isEmpty) {
          throw DataIntegrityException(s"data_mining_state of the DataMiningModel cannot be updated after the model training results are received from the Agents. " +
            s"model_id:${newDataMiningModel.model_id.get}")
        }
        logger.debug(s"handleTrainingState finished for this DataMiningModel:${dataMiningModel.model_id.get} Its state is ${newDataMiningModel.data_mining_state.get}.")
        Done
      }

    }
  }

  /**
   * Handle the processing of a DataMiningModel whose is in DataMiningState.VALIDATING state.
   *
   * @param dataMiningModel
   * @return
   */
  private def handleValidationState(dataMiningModel: DataMiningModel): Future[Done] = {
    logger.debug(s"The state of DataMiningModel:${dataMiningModel.model_id.get} is VALIDATING. It is now being processed by handleValidationState.")
    DistributedDataMiningManager.askAgentsModelValidationResults(dataMiningModel) flatMap { modelValidationResults =>
      // ModelValidationResults of the Agents whose validation is completed.
      // Others have not finished yet.

      // A ModelValidationResult includes a sequence of AlgorithmStatistics (one for each Algorithm)

      val updatedBoostedModels = dataMiningModel.algorithms.map { algorithm => // For each algorithm of this dataMiningModel
        // Find the BoostedModel of this Algorithm (I could have loop over the BoostedModels,
        // but I wanted it to be in a similar shape with handleTrainingState)

        // Let's do some integrity checks
        if (dataMiningModel.boosted_models.isEmpty) {
          throw DataIntegrityException(s"There are no BoostedModels for this DataMiningModel:${dataMiningModel.model_id.get}." +
            s"I am trying to the process handleValidationState and there must have been a BoostedModel for each Algorithm.")
        }

        val boostedModelOption = dataMiningModel.boosted_models.get.find(_.algorithm.name == algorithm.name)
        if (boostedModelOption.isEmpty) {
          throw DataIntegrityException(s"Sweet Jesus! I am processing the handleValidationState of this DataMiningModel:${dataMiningModel.model_id.get}, " +
            s"however there is no corresponding BoostedModel for this Algorithm:${algorithm.name}")
        }

        val boostedModel = boostedModelOption.get
        val updatedWeakModels = boostedModel.weak_models.map { weakModel => // For each WeakModel within this BoostedModel
          val validationStatistics = modelValidationResults
            .filterNot(_.agent.agent_id == weakModel.agent.agent_id) // Find the ModelValidationResults received from the Agents other than the training Agent of this WeakModel (we are looping over the WeakModels)
            .map { result => // For each such ModelValidationResult
              // find the AgentAlgorithmStatistics such that the agent who trained the fitted_model will be this weakModel's agent for the same algorithm
              val validationStatistics = result.validation_statistics.find(vs => vs.agent_model.agent_id == weakModel.agent.agent_id && vs.algorithm.name == weakModel.algorithm.name)
              if (validationStatistics.isEmpty) {
                throw DataIntegrityException(s"We have a problem here!!! I cannot find the validation statistics for Algorithm:${algorithm.name} " +
                  s"which was trained on Agent:${weakModel.agent} among the validation results coming from ${result.agent} for DataMiningModel:${dataMiningModel.model_id.get}.")
              }
              validationStatistics.get
            }
          val newWeakModel = weakModel.addNewValidationStatistics(validationStatistics)
          newWeakModel
        }
        boostedModel.replaceWeakModels(updatedWeakModels)
      }

      var newDataMiningModel = dataMiningModel.withBoostedModels(updatedBoostedModels)

      if (DataMiningModelController.getAgentsWaitedForValidationResults(newDataMiningModel).isEmpty) {
        // If there are no remaining Agents to wait for the validation results,
        // We can calculate the calculated_training_statistics and weights of the WeakModels.
        // Then we can call the model testing endpoints of the Agents and advance to the TESTING state

        logger.debug("There are no remaining Agents being waited for validation results. So, I will invoke the test endpoints of the Agents and " +
          s"update the state to TESTING for this DataMiningModel:${dataMiningModel.model_id.get}")
        try {
          // Calculate the calculated_training_statistics and weights of all WeakModels of all BoostedModels within this DataMiningModel
          newDataMiningModel = Aggregator.aggregate(newDataMiningModel)
        } catch {
          case e: Exception => logger.error(s"It seems there is a data integrity issues with the DataMiningModel:${newDataMiningModel.model_id.get} " +
            s"Since this is a Prediction model, training_statistics and validation_statistics MUST exist at this point.", e)
        }

        val f = DistributedDataMiningManager.invokeAgentsModelTesting(newDataMiningModel)
        try { // Wait for the testing invocations finish for all Agents
          Await.result(f, Duration(30, TimeUnit.SECONDS))
        } catch {
          case e: java.util.concurrent.TimeoutException =>
            logger.error("Invoking the model testing endpoints of {} Agents have not finished within 30 seconds " +
              "for DataMiningModel with model_id: {}.", DataMiningModelController.getSelectedAgents(dataMiningModel).length, dataMiningModel.model_id.get, e)
        }

        // Advance the state to TESTING because the Agents started testing the BoostedModels
        newDataMiningModel = newDataMiningModel.withDataMiningState(DataMiningState.TESTING)
      } else {
        logger.debug(s"There are still remaining Agents being waited for validation results for this DataMiningModel:${dataMiningModel.model_id.get}")
      }

      DataMiningModelController.updateDataMiningModel(newDataMiningModel) map { res =>
        if (res.isEmpty) {
          throw DataIntegrityException(s"data_mining_state of the DataMiningModel cannot be updated after the model validation results are received from the Agents. " +
            s"model_id:${newDataMiningModel.model_id.get}")
        }
        logger.debug(s"handleValidationState finished for this DataMiningModel:${dataMiningModel.model_id.get} Its state is ${newDataMiningModel.data_mining_state.get}.")
        Done
      }
    }
  }

  /**
   * Handle the processing of a DataMiningModel whose is in DataMiningState.TESTING state.
   *
   * @param dataMiningModel
   * @return
   */
  private def handleTestingState(dataMiningModel: DataMiningModel): Future[Done] = {
    logger.debug(s"The state of DataMiningModel:${dataMiningModel.model_id.get} is TESTING. It is now being processed by handleTestingState.")
    DistributedDataMiningManager.askAgentsModelTestResults(dataMiningModel) flatMap { modelTestResults =>
      // ModelTestResults of the Agents whose testing is completed.
      // Others have not finished yet.

      // A ModelTestResult includes a sequence of AlgorithmStatistics (one for each Algorithm)

      // Let's do some integrity checks
      if (dataMiningModel.boosted_models.isEmpty) {
        throw DataIntegrityException(s"There are no BoostedModels for this DataMiningModel:${dataMiningModel.model_id.get}." +
          s"I am trying to the process handleTestingState and there must have been BoostedModel(s) in this DataMiningModel.")
      }

      val updatedBoostedModels = dataMiningModel.boosted_models.get.map { boostedModel =>
        val testResultsOfAlgorithm = modelTestResults.map { modelTestResult =>
          val testResultsOfAlgorithmOption = modelTestResult.test_statistics.find(_.algorithm == boostedModel.algorithm)
          if (testResultsOfAlgorithmOption.isEmpty) {
            throw DataIntegrityException(s"ModelTestResult received from Agent:${modelTestResult.agent.name} for DataMiningModel:${modelTestResult.model_id}, " +
              s"however there is no result for the Algorithm:${boostedModel.algorithm.name}. This should not have happened!!!")
          }
          testResultsOfAlgorithmOption.get
        }
        boostedModel.addNewTestStatistics(testResultsOfAlgorithm)
      }
      var newDataMiningModel = dataMiningModel.withBoostedModels(updatedBoostedModels)

      if (DataMiningModelController.getAgentsWaitedForTestResults(newDataMiningModel).isEmpty) {
        // If there are no remaining Agents to wait for the test results,
        // We can calculate the calculated_test_statistics of the BoostedModels

        logger.debug("There are no remaining Agents being waited for test results. So, I will calculate the testing " +
          s"statistics for the BoostedModels of this DataMiningModel:${dataMiningModel.model_id.get}")

        val updatedBoostedModels = newDataMiningModel.boosted_models.get map { boostedModel =>
          val calculatedStatistics = StatisticsCalculator.combineStatistics(boostedModel.test_statistics.get) // Combine test_statistics in BoostedModel
          boostedModel.withCalculatedTestStatistics(calculatedStatistics)
        }
        newDataMiningModel = newDataMiningModel.withBoostedModels(updatedBoostedModels)

        // Advance the state to READY
        newDataMiningModel = newDataMiningModel.withDataMiningState(DataMiningState.READY)
      } else {
        logger.debug(s"There are still remaining Agents being waited for test results for this DataMiningModel:${dataMiningModel.model_id.get}")
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

        logger.debug(s"handleValidationState finished for this DataMiningModel:${dataMiningModel.model_id.get} Its state is ${newDataMiningModel.data_mining_state.get}.")

        Done
      }

    }
  }

}
