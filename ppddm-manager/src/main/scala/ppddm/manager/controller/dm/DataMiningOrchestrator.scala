package ppddm.manager.controller.dm

import java.time.Duration

import akka.Done
import akka.actor.Cancellable
import com.typesafe.scalalogging.Logger
import ppddm.core.rest.model.{DataMiningModel, DataMiningState}
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

  private def processDataMiningModel(model_id: String): Future[Done] = {
    DataMiningModelController.getDataMiningModel(model_id) flatMap { dataMiningModelOption =>
      if(dataMiningModelOption.isEmpty) {
        throw DataIntegrityException(s"DataMiningOrchestrator cannot access DataMiningModel with model_id:${model_id}. This should not have happened!!")
      }
      val dataMiningModel = dataMiningModelOption.get
      dataMiningModel.data_mining_state match {
        case None =>
          // This is the first time of this DataMiningModel with us ;) Start the training
          DistributedDataMiningManager.invokeAgentsModelTraining(dataMiningModel) flatMap { _ =>
            DataMiningModelController.updateDataMiningModel(dataMiningModel.withDataMiningState(DataMiningState.TRAINING)) map { res =>
              if(res.isEmpty) {
                throw DataIntegrityException(s"data_mining_state of the DataMiningModel cannot be updated after the model training requests are sent to the Agents. " +
                  s"model_id:${dataMiningModel.model_id.get}")
              }
              Done
            }
          }
        case Some(DataMiningState.TRAINING) =>
          // This DataMiningModel is still training the fitted_models on Agents. Ask to the Agents whether they are completed or not.
          // If they are all completed, create the WeakModels and validate their fitted_models on other Agents
          Future.apply(Done)
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

}
