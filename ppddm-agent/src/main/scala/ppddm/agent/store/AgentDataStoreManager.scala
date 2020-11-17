package ppddm.agent.store

import java.util.UUID

import ppddm.agent.config.AgentConfig
import ppddm.agent.controller.dm.DataMiningRequestType
import ppddm.agent.controller.dm.DataMiningRequestType.DataMiningRequestType
import ppddm.core.store.DataStoreManager


/**
 * Manages the persistent storage of the Dataframes as parquet files
 */
object AgentDataStoreManager extends DataStoreManager {
  // Place AgentID on ppddm store path to avoid the problem that will occur when agents working on the same machine trying to edit the same files.
  final private val BASE_AGENT_DIR: String = BASE_STORE_DIR + AgentConfig.agentID
  final private val DS_STORE_DIR: String = BASE_AGENT_DIR + "/datasets/"
  final private val STAT_STORE_DIR: String = BASE_AGENT_DIR + "/statistics/"
  final private val MODEL_TRAIN_STORE_DIR: String = BASE_AGENT_DIR + "/models/train/"
  final private val MODEL_VALIDATE_STORE_DIR: String = BASE_AGENT_DIR + "/models/validate/"
  final private val MODEL_TEST_STORE_DIR: String = BASE_AGENT_DIR + "/models/test/"
  final private val TMP_STORE_DIR: String = BASE_AGENT_DIR + "/tmp/"

  /**
   * Returns the path to the location where the dataset with the given dataset_id is kept.
   * /ppddm-store/datasets/:dataset_id
   *
   * @param dataset_id
   * @return
   */
  def getDatasetPath(dataset_id: String): String = {
    DS_STORE_DIR + dataset_id
  }

  /**
   * Returns the path to the location where the statistics with the given dataset_id is kept.
   * /ppddm-store/statistics/:dataset_id
   *
   * @param dataset_id
   * @return
   */
  def getStatisticsPath(dataset_id: String): String = {
    STAT_STORE_DIR + dataset_id
  }

  /**
   * Returns the path to the location where the model with the given model_id is kept.
   * /ppddm-store/models/:model_id
   *
   * @param model_id
   * @param dataMiningRequestType
   * @return
   */
  def getModelPath(model_id: String, dataMiningRequestType: DataMiningRequestType): String = {
    dataMiningRequestType match {
      case DataMiningRequestType.TRAIN => MODEL_TRAIN_STORE_DIR + model_id
      case DataMiningRequestType.VALIDATE => MODEL_VALIDATE_STORE_DIR + model_id
      case DataMiningRequestType.TEST => MODEL_TEST_STORE_DIR + model_id
    }
  }

  /**
   * Generates and returns a unique path under the directory of temporary files.
   *
   * @return
   */
  def getTmpPath(): String = {
    TMP_STORE_DIR + UUID.randomUUID().toString
  }

}
