package ppddm.manager.controller.dm

import ppddm.manager.Manager
import ppddm.core.ai.RegressionHandler

object DataMiningController {

  def testRegression(): Seq[String] = {
    new RegressionHandler(Manager.dataMiningEngine.sparkSession).test()
  }

}
