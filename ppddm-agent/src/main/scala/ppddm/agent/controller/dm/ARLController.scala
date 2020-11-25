package ppddm.agent.controller.dm

import akka.Done
import ppddm.agent.exception.DataMiningException
import ppddm.agent.store.AgentDataStoreManager
import ppddm.core.rest.model.{ARLExecutionRequest, ARLExecutionResult, ARLFrequencyCalculationRequest, ARLFrequencyCalculationResult, DataType, Parameter}
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
      val dataFrame = retrieveDataFrame(frequencyCalculationRequest.dataset_id)

      logger.debug(s"DataFrame for the Dataset:${frequencyCalculationRequest.dataset_id} is retrieved for frequency calculation in DataMiningModel:${frequencyCalculationRequest.model_id}")

      // TODO how to handle categorical variables in here? Actually, how to handle non-binary columns?

      logger.debug(s"Calculating item frequencies...")
      val itemFrequencies = dataFrame.schema.toSeq.tail map { s =>
        val count = dataFrame.filter(s"${s.name} != 0.0").count()
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

  def startExecution(arlExecutionRequest: ARLExecutionRequest): Future[Done] = {
    Future { Done } // TODO
  }

  def getExecutionResult(model_id: String): Option[ARLExecutionResult] = {
    null // TODO
  }

  def deleteExecutionResult(model_id: String): Option[Done] = {
    Some(Done) // TODO
  }

}
