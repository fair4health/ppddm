package ppddm.agent.controller.dm.algorithm.arl

import ppddm.agent.controller.dm.algorithm.DataMiningAlgorithm
import ppddm.agent.exception.DataMiningException
import ppddm.core.rest.model.{Agent, Algorithm, AlgorithmName}

trait ARLAlgorithm extends DataMiningAlgorithm {

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
