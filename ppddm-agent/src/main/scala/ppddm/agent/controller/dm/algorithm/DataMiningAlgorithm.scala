package ppddm.agent.controller.dm.algorithm

import com.typesafe.scalalogging.Logger
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import ppddm.agent.store.AgentDataStoreManager
import ppddm.core.ai.PipelineModelEncoderDecoder
import ppddm.core.rest.model.{Agent, Algorithm}

trait DataMiningAlgorithm {

  implicit val sparkSession: SparkSession = ppddm.agent.Agent.dataMiningEngine.sparkSession
  protected val logger: Logger = Logger(this.getClass)

  protected val agent: Agent // on which aAgent this DataMiningAlgorithm is running now
  protected val algorithm: Algorithm // The Algoritm that this DataMiningAlgorithm is training/validating/testing

  /**
   * Generate a Base64 encoded string of the fitted model of this algorithm
   *
   * @param model A PipelineModel
   * @return
   */
  def toString(model: PipelineModel): String = {
    PipelineModelEncoderDecoder.toString(model, AgentDataStoreManager.getTmpPath())
  }

  /**
   * Creates a PipelineModel from the Base64 encoded fitted_model string of this algorithm.
   *
   * @param modelString Base64 encoded string representation of the PipelineModel file content
   * @return A PipelineModel
   */
  def fromString(modelString: String): PipelineModel = {
    PipelineModelEncoderDecoder.fromString(modelString, AgentDataStoreManager.getTmpPath())
  }

}
