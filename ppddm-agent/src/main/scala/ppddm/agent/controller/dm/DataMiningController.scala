package ppddm.agent.controller.dm

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import ppddm.agent.Agent
import ppddm.agent.exception.DataMiningException
import ppddm.agent.store.AgentDataStoreManager

/**
 * Base Controller object for Data Mining Algorithm Execution
 */
trait DataMiningController {

  protected val logger: Logger = Logger(this.getClass)
  protected implicit val sparkSession: SparkSession = Agent.dataMiningEngine.sparkSession

  /**
   * Retrieves already saved DataFrame from DataStore
   * @param dataset_id the id of dataset to be retrieved
   * @return the DataFrame if it exists. If not, throws a DataMiningException
   */
  def retrieveDataFrame(dataset_id: String): DataFrame = {
    val dataFrameOption = AgentDataStoreManager.getDataFrame(AgentDataStoreManager.getDatasetPath(dataset_id))
    if (dataFrameOption.isEmpty) {
      val msg = s"The Dataset with id:${dataset_id} on which Data Mining algorithms will be executed does not exist. This should not have happened!!"
      logger.error(msg)
      throw DataMiningException(msg)
    }
    dataFrameOption.get
  }
}
