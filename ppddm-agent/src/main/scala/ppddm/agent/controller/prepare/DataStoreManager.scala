package ppddm.agent.controller.prepare

import org.apache.spark.sql.{DataFrame, SparkSession}
import ppddm.agent.Agent

import scala.util.Try

/**
 * Manages the persistent storage of the Dataframes as parquet files
 */
object DataStoreManager {

  final private val DS_STORE_DIR: String = "ppddm-store/datasets/"
  final private val STAT_STORE_DIR: String = "ppddm-store/statistics/"

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