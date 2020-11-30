package ppddm.manager.controller.dm

import akka.Done
import akka.actor.Cancellable
import com.typesafe.scalalogging.Logger
import ppddm.core.rest.model.{BoostedModel, DataMiningModel, DataMiningState, ProjectType}
import ppddm.manager.exception.DataIntegrityException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import java.util.concurrent.TimeUnit

import ppddm.core.ai.{Aggregator, StatisticsCalculator}
import ppddm.manager.controller.project.ProjectController

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

  def stopOrchestration(model_id: String): Unit = {
    logger.debug(s"Stopping the orchestration for this DataMiningModel:${model_id} since it is in FINAL state.")
    val scheduledProcess = scheduledProcesses.get(model_id)
    if (scheduledProcess.isEmpty) {
      logger.error(s"There is no Scheduled Process to stop for this DataMiningModel:${model_id}")
    }
    if (!scheduledProcess.get.cancel && !scheduledProcess.get.isCancelled) {
      logger.error(s"Cancellation of the scheduled process for the DataMiningModel:${model_id} is UNSUCCESSFUL")
    }

    if (scheduledProcess.get.isCancelled) {
      logger.debug(s"Orchestration (the scheduled process) for DataMiningModel:${model_id} is stopped.")
    }
  }

  def startOrchestration(dataMiningModel: DataMiningModel): Unit = {
    /* Import the ActorSystem */
    import ppddm.manager.config.ManagerExecutionContext._

    import java.time.Duration

    val newScheduledProcess = actorSystem.scheduler.scheduleWithFixedDelay(
      Duration.ZERO,
      Duration.ofSeconds(SCHEDULE_INTERVAL_SECONDS),
      () => {
        logger.debug("Scheduled processing STARTED for DataMiningModel with model_id:{} and model_name:{}", dataMiningModel.model_id.get, dataMiningModel.name)
        try {
          processDataMiningModel(dataMiningModel.model_id.get)
            .recover {
              case e: Exception =>
                logger.error(e.getMessage, e)
                throw e
            }
        } catch {
          case e: Exception => // Do nothing, it is already logged
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
    DataMiningModelController.getDataMiningModel(model_id) flatMap { dataMiningModelOption =>
      if (dataMiningModelOption.isEmpty) {
        throw DataIntegrityException(s"DataMiningOrchestrator cannot access DataMiningModel with model_id:${model_id}. This should not have happened!!")
      }
      val dataMiningModel = dataMiningModelOption.get

      // Retrieve the Project of this DataMiningModel to check its ProjectType
      ProjectController.getProject(dataMiningModel.project_id) flatMap { project =>
        if (project.isEmpty) {
          throw DataIntegrityException(s"DataMiningOrchestrator cannot access the Project of the DataMiningModel. model_id:${dataMiningModel.model_id.get} - " +
            s"project_id:${dataMiningModel.project_id} This should not have happened!!")
        }
        // Different ProjectTypes are handled by different state transitions
        project.get.project_type match {
          case ProjectType.PREDICTION => PredictionMiningProcessor.processPredictionMining(dataMiningModel)
          case ProjectType.ASSOCIATION => AssociationMiningProcessor.processAssociationMining(dataMiningModel)
          case unknownType => throw new IllegalArgumentException(s"Unknown Project Type:$unknownType")
        }
      }
    }

  }

}
