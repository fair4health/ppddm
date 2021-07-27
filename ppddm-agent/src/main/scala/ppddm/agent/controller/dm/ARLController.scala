package ppddm.agent.controller.dm

import akka.Done
import ppddm.agent.controller.dm.algorithm.arl.ARLAlgorithm
import ppddm.agent.store.AgentDataStoreManager
import ppddm.core.rest.model._
import ppddm.core.util.JsonFormatter._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

object ARLController extends DataMiningController {

  import sparkSession.implicits._

  /**
   * Start the calculation of frequencies of items for the dataset of given model. This function returns if the calculation is started successfully.
   * Frequency calculation will continue in the background and the manager will ask about the status through a separate HTTP GET call.
   *
   * @param frequencyCalculationRequest
   * @return
   */
  def startFrequencyCalculation(frequencyCalculationRequest: ARLFrequencyCalculationRequest): Future[Done] = {
    Future {
      logger.debug("ARLFrequencyCalculationRequest received on agent:{} for model:{}",
        frequencyCalculationRequest.agent.agent_id, frequencyCalculationRequest.model_id)

      // Retrieve the DataFrame object with the given dataset_id previously saved in the data store
      var dataFrame = retrieveDataFrame(frequencyCalculationRequest.dataset_id)
      logger.debug(s"DataFrame for the Dataset:${frequencyCalculationRequest.dataset_id} is retrieved for frequency calculation in DataMiningModel:${frequencyCalculationRequest.model_id}")

      dataFrame = DataAnalysisManager.performCategoricalTransformations(dataFrame)
      logger.debug(s"Categorical transformations are applied for the DataFrame...")

      logger.debug(s"Calculating item frequencies...")
      val itemFrequencies = dataFrame.schema.tail map { s =>
        val count = dataFrame.filter(dataFrame(s.name) =!= "0.0").count()
        Parameter(s.name, DataType.INTEGER, count.toString)
      }

      val frequencyCalculationResult = ARLFrequencyCalculationResult(frequencyCalculationRequest.model_id,
        frequencyCalculationRequest.dataset_id, frequencyCalculationRequest.agent, itemFrequencies, dataFrame.count())

      try {
        // Save the ARLFrequencyCalculationResult containing the models into ppddm-store/frequencies/:model_id
        AgentDataStoreManager.saveDataFrame(
          AgentDataStoreManager.getFrequenciesPath(frequencyCalculationRequest.model_id),
          Seq(frequencyCalculationResult.toJson).toDF())
        logger.debug(s"ARLFrequencyCalculationResult has been created and persisted into the data store successfully for DataMiningModel:${frequencyCalculationRequest.model_id}")
        Done
      } catch {
        case e: Exception =>
          val msg = s"Cannot save the ARLFrequencyCalculationResult of the model with model_id: ${frequencyCalculationRequest.model_id}."
          logger.error(msg)
          throw DataMiningException(msg, e)
      }

      Done
    }
  }

  /**
   * Retrieves the ARLFrequencyCalculationResult which includes the item frequencies and total count
   *
   * @param model_id
   * @return
   */
  def getFrequencyCalculationResult(model_id: String): Option[ARLFrequencyCalculationResult] = {
    logger.debug("getFrequencyCalculationResult received on for model:{}", model_id)
    Try(
      AgentDataStoreManager.getDataFrame(AgentDataStoreManager.getFrequenciesPath(model_id)) map { df =>
        df // Dataframe consisting of a column named "value" that holds Json inside
          .head() // Get the Array[Row]
          .getString(0) // Get Json String
          .extract[ARLFrequencyCalculationResult]
      }).getOrElse(None) // Returns None if an error occurs within the Try block
  }

  /**
   * Deletes ARLFrequencyCalculationResult
   *
   * @param model_id The unique identifier of the model whose ARLFrequencyCalculationResult is to be deleted
   * @return
   */
  def deleteFrequencyCalculationResult(model_id: String): Option[Done] = {
    logger.debug("deleteFrequencyCalculationResult received on for model:{}", model_id)
    if (AgentDataStoreManager.deleteDirectory(AgentDataStoreManager.getFrequenciesPath(model_id))) {
      logger.info(s"ARLFrequencyCalculationResult of model (with id: $model_id) have been deleted successfully")
      Some(Done)
    } else {
      logger.debug(s"ARLFrequencyCalculationResult of model (with id: $model_id) do not exist!")
      Option.empty[Done]
    }
  }

  /**
   * Start the execution of the data mining algorithm for ARL. This function returns if the execution is started successfully.
   * Model execution will continue in the background and the manager will ask about the status through a separate HTTP GET call.
   *
   * @param arlExecutionRequest
   * @return
   */
  def startExecution(arlExecutionRequest: ARLExecutionRequest): Future[Done] = {
    logger.debug("ARLExecutionRequest received on agent:{} for model:{}",
      arlExecutionRequest.agent.agent_id, arlExecutionRequest.model_id)

    // Retrieve the DataFrame object with the given dataset_id previously saved in the data store
    var dataFrame = retrieveDataFrame(arlExecutionRequest.dataset_id)
    logger.debug(s"DataFrame for the Dataset:${arlExecutionRequest.dataset_id} is retrieved for ARL execution in DataMiningModel:${arlExecutionRequest.model_id}")

    dataFrame = DataAnalysisManager.performCategoricalTransformations(dataFrame)
    logger.debug(s"Categorical transformations are applied for the DataFrame...")

    // Execute algorithms on the frequentItemDataFrame
    val executionFutures = arlExecutionRequest.algorithm_set map { algorithmItemPair =>
      // First eliminate items that do not exist in the data frame
      val filteredItems = algorithmItemPair.items.filter(dataFrame.schema.fieldNames.contains(_))

      // Keep only the frequent items sent in the request in the data frame
      val frequentItemDataFrame = dataFrame.select(filteredItems.head, filteredItems.tail: _*)

      ARLAlgorithm(arlExecutionRequest.agent, algorithmItemPair.algorithm).execute(frequentItemDataFrame)
    }

    Future.sequence(executionFutures) map { arl_models => // Join the Futures
      logger.debug(s"Parallel execution of the Algorithms have been completed for DataMiningModel:${arlExecutionRequest.model_id}")

      val arlExecutionResult = ARLExecutionResult(arlExecutionRequest.model_id, arlExecutionRequest.dataset_id,
        arlExecutionRequest.agent, arl_models)
      try {
        // Save the ARLExecutionResult containing the models into ppddm-store/models/:model_id
        AgentDataStoreManager.saveDataFrame(
          AgentDataStoreManager.getModelPath(arlExecutionRequest.model_id, DataMiningRequestType.ARL),
          Seq(arlExecutionResult.toJson).toDF())
        logger.debug(s"ARLExecutionResult has been created and persisted into the data store successfully for DataMiningModel:${arlExecutionRequest.model_id}")
        Done
      } catch {
        case e: Exception =>
          val msg = s"Cannot save the ARLExecutionResult of the model with model_id: ${arlExecutionRequest.model_id}."
          logger.error(msg)
          throw DataMiningException(msg, e)
      }
    }
  }

  /**
   * Retrieves the ARLExecutionResult which includes the ARL models for the algorithms that were executed
   *
   * @param model_id
   * @return
   */
  def getExecutionResult(model_id: String): Option[ARLExecutionResult] = {
    logger.debug("getExecutionResult received on for model:{}", model_id)
    Try(
      AgentDataStoreManager.getDataFrame(AgentDataStoreManager.getModelPath(model_id, DataMiningRequestType.ARL)) map { df =>
        df // Dataframe consisting of a column named "value" that holds Json inside
          .head() // Get the Array[Row]
          .getString(0) // Get Json String
          .extract[ARLExecutionResult]
      }).getOrElse(None) // Returns None if an error occurs within the Try block
  }

  /**
   * Deletes ARLExecutionResult
   *
   * @param model_id The unique identifier of the model whose ARLExecutionResult is to be deleted
   * @return
   */
  def deleteExecutionResult(model_id: String): Option[Done] = {
    logger.debug("deleteExecutionResult received on for model:{}", model_id)
    if (AgentDataStoreManager.deleteDirectory(AgentDataStoreManager.getModelPath(model_id, DataMiningRequestType.ARL))) {
      logger.info(s"ARLExecutionResult of model (with id: $model_id) have been deleted successfully")
      Some(Done)
    } else {
      logger.debug(s"ARLExecutionResult of model (with id: $model_id) do not exist!")
      Option.empty[Done]
    }
  }

}
