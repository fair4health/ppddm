package ppddm.agent.controller.dm

import akka.Done
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import ppddm.agent.Agent
import ppddm.core.rest.model.{ARLExecutionRequest, ARLExecutionResult, ARLFrequencyCalculationRequest, ARLFrequencyCalculationResult}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object ARLController {

  private val logger: Logger = Logger(this.getClass)
  private implicit val sparkSession: SparkSession = Agent.dataMiningEngine.sparkSession

  def startFrequencyCalculation(frequencyCalculationRequest: ARLFrequencyCalculationRequest): Future[Done] = {
    Future { Done } // TODO
  }

  def getFrequencyCalculationResult(model_id: String): Option[ARLFrequencyCalculationResult] = {
    null // TODO
  }

  def deleteFrequencyCalculationResult(model_id: String): Option[Done] = {
    Some(Done) // TODO
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
