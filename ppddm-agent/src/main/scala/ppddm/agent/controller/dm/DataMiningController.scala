package ppddm.agent.controller.dm

import akka.Done
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import ppddm.agent.Agent
import ppddm.agent.controller.prepare.DataStoreManager
import ppddm.agent.exception.AlgorithmExecutionException
import ppddm.core.rest.model.{AlgorithmExecutionRequest, AlgorithmExecutionResult}
import ppddm.core.util.JsonFormatter._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

object DataMiningController {

  private val logger: Logger = Logger(this.getClass)
  private val sparkSession: SparkSession = Agent.dataMiningEngine.sparkSession

  import sparkSession.implicits._

  def startAlgorithmExecution(algorithmExecutionRequest: AlgorithmExecutionRequest): Future[Done] = {
    logger.debug("Algorithm execution request received.")

    // First prepare the data for algorithm execution, i.e. perform the exploratory data analysis which include categorical variable handling, null values handling etc.
    val df = DataAnalysisManager.performDataAnalysis(algorithmExecutionRequest.dataset_id)

    // Then execute the algorithms one by one on the same data
    val algorithmModelFutures = algorithmExecutionRequest.algorithms map { algorithm =>
      AlgorithmExecutionManager.executeAlgorithm(algorithmExecutionRequest.agent, algorithm, df)
    }

    // Save the AlgorithmExecutionResult containing the models into ppddm-store/models/:model_id
    Future.sequence(algorithmModelFutures) map { algorithm_models =>
      val result = AlgorithmExecutionResult(algorithmExecutionRequest.model_id, algorithmExecutionRequest.dataset_id,
        algorithmExecutionRequest.agent, algorithm_models)
      try {
        DataStoreManager.saveDF(
          DataStoreManager.getModelPath(algorithmExecutionRequest.model_id),
          Seq(result.toJson).toDF())

        Done
      }
      catch {
        case e: Exception =>
          val msg = s"Cannot save the AlgorithmExecutionResult of the model with id: ${algorithmExecutionRequest.model_id}."
          logger.error(msg)
          throw AlgorithmExecutionException(msg, e)
      }
    }
  }

  /**
   * Retrieves the AlgorithmExecutionResult which includes the models and statistics for the algorithms that were executed
   *
   * @param model_id
   * @return
   */
  def getAlgorithmExecutionResult(model_id: String): Option[AlgorithmExecutionResult] = {
    Try(
      DataStoreManager.getDF(DataStoreManager.getModelPath(model_id)) map { df =>
        df // Dataframe consisting of a column named "value" that holds Json inside
          .head() // Get the Array[Row]
          .getString(0) // Get Json String
          .extract[AlgorithmExecutionResult]
      }).getOrElse(None) // Returns None if an error occurs within the Try block
  }

  /**
   * Deletes AlgorithmExecutionResult
   *
   * @param model_id The unique identifier of the model whose AlgorithmExecutionResult is to be deleted
   * @return
   */
  def deleteAlgorithmExecutionResult(model_id: String): Option[Done] = {
    // Delete the AlgorithmExecutionResult with the given model_id
    val resulttDeleted = DataStoreManager.deleteDF(DataStoreManager.getModelPath(model_id))
    if (resulttDeleted) {
      logger.info(s"AlgorithmExecutionResult of model (with id: $model_id) have been deleted successfully")
      Some(Done)
    } else {
      logger.info(s"AlgorithmExecutionResult of model (with id: $model_id) do not exist!")
      Option.empty[Done]
    }
  }
}
