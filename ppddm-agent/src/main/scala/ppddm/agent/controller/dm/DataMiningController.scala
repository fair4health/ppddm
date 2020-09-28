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
    try {
      Future.sequence(algorithmModelFutures) map { algorithm_models =>
        val result = AlgorithmExecutionResult(algorithmExecutionRequest.model_id, algorithmExecutionRequest.dataset_id,
          algorithmExecutionRequest.agent, algorithm_models)
        DataStoreManager.saveDF(
          DataStoreManager.getModelPath(algorithmExecutionRequest.model_id),
          Seq(result.toJson).toDF())
      }
    }
    catch {
      case e: Exception =>
        val msg = s"Cannot save the AlgorithmExecutionResult of the model with id: ${algorithmExecutionRequest.model_id}."
        logger.error(msg)
        throw AlgorithmExecutionException(msg, e)
    }

    Future { Done } // TODO tamamını mı Future alacağız?
  }

}
