package ppddm.agent.controller.dm.algorithm.arl

import ppddm.core.rest.model.{Agent, Algorithm}

case class FPGrowthPPDDM(override val agent: Agent, override val algorithm: Algorithm) extends ARLAlgorithm {

}
