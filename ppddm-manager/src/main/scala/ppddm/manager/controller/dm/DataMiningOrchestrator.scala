package ppddm.manager.controller.dm

import java.time.Duration

import akka.Done
import akka.actor.Cancellable
import com.typesafe.scalalogging.Logger
import ppddm.core.rest.model.{BoostedModel, DataMiningModel, DataMiningState}
import ppddm.manager.exception.DataIntegrityException

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

  private val logger: Logger = Logger(this.getClass)

  private val SCHEDULE_INTERVAL_SECONDS = 120L

  // Keep the record of scheduled processes
  private var scheduledProcesses = Map.empty[String, Cancellable]

  def startOrchestration(dataMiningModel: DataMiningModel): Unit = {
    /* Import the ActorSystem */
    import ppddm.manager.config.ManagerExecutionContext._
    val newScheduledProcess = actorSystem.scheduler.scheduleWithFixedDelay(
      Duration.ZERO,
      Duration.ofSeconds(SCHEDULE_INTERVAL_SECONDS),
      () => {
        logger.debug("Scheduled processing for DataMiningModel with model_id:{} and model_name:{}", dataMiningModel.model_id.get, dataMiningModel.name)
        try {
          processDataMiningModel(dataMiningModel.model_id.get)
        } catch {
          case e: Exception =>
            logger.error(e.getMessage, e)
        }
      },
      actorSystem.dispatcher)
    scheduledProcesses += (dataMiningModel.model_id.get -> newScheduledProcess)
  }

  /**
   * Handles the processing of the DataMiningModel indicated by model_id by respecting to its DataMiningState.
   *
   * @param model_id
   * @return
   */
  private def processDataMiningModel(model_id: String): Future[Done] = {
    // First, retrieve the DataMiningModel from the database
    // Handle the processing based on its DataMiningState
    DataMiningModelController.getDataMiningModel(model_id) flatMap { dataMiningModelOption =>
      if (dataMiningModelOption.isEmpty) {
        throw DataIntegrityException(s"DataMiningOrchestrator cannot access DataMiningModel with model_id:${model_id}. This should not have happened!!")
      }
      val dataMiningModel = dataMiningModelOption.get
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
          Future.apply(Done)
        case Some(DataMiningState.TESTING) =>
          // This DataMiningModel has a BoostedModel which is being tested on the Agents. Ask to the Agents whether they are completed or not.
          // If they are all completed, we can finalize the DataMiningModel
          // After this block, the scheduled process should be removed/cancelled
          Future.apply(Done)
        case Some(DataMiningState.FINAL) =>
          // This is already in its FINAL state, this block should not execute in normal circumstances.
          Future.apply(Done)
      }
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
    DistributedDataMiningManager.invokeAgentsModelTraining(dataMiningModel) flatMap { _ =>
      // After invoking the training endpoints of all Agents, update the state of the DataMiningController in the database
      DataMiningModelController.updateDataMiningModel(dataMiningModel.withDataMiningState(DataMiningState.TRAINING)) map { res =>
        if (res.isEmpty) {
          throw DataIntegrityException(s"data_mining_state of the DataMiningModel cannot be updated after the model training requests are sent to the Agents. " +
            s"model_id:${dataMiningModel.model_id.get}")
        }
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
    DistributedDataMiningManager.askAgentsModelTrainingResults(dataMiningModel) flatMap { results =>
      // results include the ModelTrainingResults of the Agents whose training is completed.
      // Others have not finished yet.

      // A ModelTrainingResult includes a sequence of WeakModel (one for each Algorithm)
      // All ModelTrainingResults should include the same sequence of WeakModels

      val newBoostedModels = dataMiningModel.algorithms.map { algorithm => // For each algorithm of this dataMiningModel

        val weakModelsOfAlgorithm = results.map { modelTrainingResult => // find the corresponding WeakModel returned by each Agent and make a sequence of it
          val wm = modelTrainingResult.algorithm_training_models.find(_.algorithm.name == algorithm.name)
          if (wm.isEmpty) {
            throw DataIntegrityException(s"The Algorithm with name ${algorithm.name} could not be found on the WeakModel " +
              s"training results of ${modelTrainingResult.agent} for the DataMiningModel with model_id:${dataMiningModel.model_id} and name:${dataMiningModel.name}")
          }
          wm.get
        }

        // Check whether this DataMiningModel has already a BoostedModel for this algorithm (look up, we are looping over the algorithms)
        val existingBoostedModel = if(dataMiningModel.boosted_models.isDefined) dataMiningModel.boosted_models.get.find(_.algorithm.name == algorithm.name) else None

        if(existingBoostedModel.isDefined) {
          // This means we previously created a BoostedModel for this algorithm and saved into the database.
          // Now, new WeakModels arrived from different Agents for this BoostedModel (look up, we are looping over the algorithms)
          logger.debug("There is already a BoostedModel for this Algorithm:{} under the DataMiningModel with model_id:{}. " +
            "Newcoming WeakModels will be added to that BoostedModel.", algorithm.name, dataMiningModel.model_id.get)
          existingBoostedModel.get.withNewWeakModels(weakModelsOfAlgorithm)
        } else {
          // This means this is the first time that we will create a BoostedModel for this algorithm
          logger.debug("Creating a BoostedModel for the 1st time for the Algorithm:{} under the DataMiningModel with model_id:{}",
            algorithm.name, dataMiningModel.model_id.get)
          BoostedModel(algorithm, weakModelsOfAlgorithm, None, None) // Create the BoostedModel for this algorithm for the first time
        }

      }

      var newDataMiningModel = dataMiningModel.withBoostedModels(newBoostedModels)
      if(DataMiningModelController.getAgentsWaitedForTrainingResults(newDataMiningModel).isEmpty) {
        // If there are no remaining Agents to wait for the training results, then we can advance to the VALIDATING state
        newDataMiningModel = newDataMiningModel.withDataMiningState(DataMiningState.VALIDATING)
      }

      // After processing the received ModelTrainingResult, save the newDataMiningModel into the database
      DataMiningModelController.updateDataMiningModel(newDataMiningModel) map { res =>
        if (res.isEmpty) {
          throw DataIntegrityException(s"data_mining_state of the DataMiningModel cannot be updated after the model training results are received from the Agents. " +
            s"model_id:${newDataMiningModel.model_id.get}")
        }
        Done
      }

    }

  }

}
