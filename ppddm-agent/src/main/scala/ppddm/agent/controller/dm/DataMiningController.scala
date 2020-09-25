package ppddm.agent.controller.dm

import akka.Done
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.json4s.JsonAST.JObject
import ppddm.agent.Agent
import ppddm.agent.controller.prepare.{DataPreparationController, DataStoreManager}
import ppddm.core.rest.model.{AlgorithmExecutionRequest, DataPreparationResult}

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
    algorithmExecutionRequest.algorithms map { algorithm =>
      AlgorithmExecutionManager.executeAlgorithm(algorithm, df)
    }

    df.show(false)

    null
  }

}
