package ppddm.agent.controller.prepare

import org.apache.spark.sql.{DataFrame, SparkSession}
import ppddm.agent.Agent

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

object DataStoreManager {

  final private val DS_STORE_DIR: String = "ppddm-store/datasets/"
  final private val STAT_STORE_DIR: String = "ppddm-store/statistics/"

  private val sparkSession: SparkSession = Agent.dataMiningEngine.sparkSession

  /**
   * Saves the DataFrame to the given location
   *
   * @param path
   * @param df
   * @return
   */
  def saveDF (path: String, df: DataFrame): Future[Unit] = {
    Future(df.write.parquet(path))
  }

  /**
   * Retrieves the DataFrame at the given location
   *
   * @param path
   * @return
   */
  def getDF (path: String): Future[Option[DataFrame]] = {
    Future(Try(sparkSession.read.parquet(path)).toOption)
  }

  /**
   * Returns the path to the location where the dataset with the given dataset_id is kept.
   * /ppddm-store/datasets/:dataset_id
   *
   * @param dataset_id
   * @return
   */
  def getDatasetPath (dataset_id: String): String = {
    DS_STORE_DIR + dataset_id
  }

  /**
   * Returns the path to the location where the statistics with the given dataset_id is kept.
   * /ppddm-store/statistics/:dataset_id
   *
   * @param dataset_id
   * @return
   */
  def getStatisticsPath (dataset_id: String): String = {
    STAT_STORE_DIR + dataset_id
  }

}
