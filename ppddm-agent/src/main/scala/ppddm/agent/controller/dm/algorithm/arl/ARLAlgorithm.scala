package ppddm.agent.controller.dm.algorithm.arl

import org.apache.spark.sql.DataFrame
import ppddm.agent.controller.dm.algorithm.DataMiningAlgorithm
import ppddm.core.rest.model.{ARLModel, Agent, Algorithm, AlgorithmName, DataMiningException}

import scala.concurrent.Future

trait ARLAlgorithm extends DataMiningAlgorithm {

  def execute(frequentItemDataFrame: DataFrame): Future[ARLModel]

}

object ARLAlgorithm {
  def apply(agent: Agent, algorithm: Algorithm): ARLAlgorithm = {
    algorithm.name match {
      case AlgorithmName.ARL_FPGROWTH => FPGrowthPPDDM(agent, algorithm)
      case _ =>
        val msg = s"Unknown Algorithm:${algorithm.name}"
        throw DataMiningException(msg)
    }
  }
}
