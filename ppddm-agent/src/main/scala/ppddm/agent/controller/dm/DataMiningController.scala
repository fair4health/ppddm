package ppddm.agent.controller.dm

import ppddm.agent.Agent
import ppddm.core.ai.RegressionHandler

object DataMiningController {

  def testRegression(): Seq[String] = {
    new RegressionHandler(Agent.dataMiningEngine.sparkSession).test()
  }

}
