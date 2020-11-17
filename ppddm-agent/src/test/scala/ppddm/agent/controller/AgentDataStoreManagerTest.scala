package ppddm.agent.controller

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import ppddm.agent.SparkSessionWrapper
import ppddm.agent.store.AgentDataStoreManager

@RunWith(classOf[JUnitRunner])
class AgentDataStoreManagerTest extends Specification with SparkSessionWrapper {
  import sparkSession.implicits._

  sequential

  "Data Store Manager" should {
    "save and get the dataframe" in {
      // Create a simple dataframe
      val df = Seq(1,2,3).toDF("numbers")
      // Save the dataframe with DataStoreManager
      AgentDataStoreManager.saveDataFrame(AgentDataStoreManager.getDatasetPath("test-df"), df)

      // Try to get saved dataframe with id
      val currentDF = AgentDataStoreManager.getDataFrame(AgentDataStoreManager.getDatasetPath("test-df")).get
      // Check whether the actual and expected df are equal or not
      df.except(currentDF).count() shouldEqual 0
    }

    "delete the dataframe created" in {
      // Delete the created dataframe above
      AgentDataStoreManager.deleteDirectory(AgentDataStoreManager.getDatasetPath("test-df")) shouldEqual true
    }

    "reject to delete non-existing dataframe" in {
      // Try to delete a dataframe which does not exist in the store
      AgentDataStoreManager.deleteDirectory(AgentDataStoreManager.getDatasetPath("test-df1")) shouldEqual false
    }
  }

}
