package ppddm.agent.controller.prepare

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}
import ppddm.agent.Agent
import ppddm.agent.config.AgentConfig

import scala.reflect.io.Directory
import scala.util.Try

/**
 * Manages the persistent storage of the Dataframes as parquet files
 */
object DataStoreManager {
  // Place AgentID on ppddm store path to avoid the problem that will occur when agents working on the same machine trying to edit the same files.
  final private val BASE_STORE_DIR: String = "ppddm-store/" + AgentConfig.agentID
  final private val DS_STORE_DIR: String = BASE_STORE_DIR + "/datasets/"
  final private val STAT_STORE_DIR: String = BASE_STORE_DIR + "/statistics/"

  private val sparkSession: SparkSession = Agent.dataMiningEngine.sparkSession

  /**
   * Saves the DataFrame to the given file path
   *
   * @param path The filepath to save the DataFrame
   * @param df The DataFrame to be saved
   * @return
   */
  def saveDF(path: String, df: DataFrame): Unit = {
    df.write.parquet(path)
  }

  /**
   * Retrieves the DataFrame from the given file path
   *
   * @param path The filepath of the DataFrame
   * @return the DataFrame if it is found
   */
  def getDF(path: String): Option[DataFrame] = {
    Try(sparkSession.read.parquet(path)).toOption
  }

  /**
   * Deletes the files under the given path recursively.
   *
   * @param path The filepath of the DataFrame
   */
  def deleteDF(path: String): Unit = {
    new Directory(new File(path)).deleteRecursively()
  }

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

}
