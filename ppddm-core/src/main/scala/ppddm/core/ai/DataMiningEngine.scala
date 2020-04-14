package ppddm.core.ai

import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Data Mining Engine which handles all Spark processing required for the data mining algorithms
 */
class DataMiningEngine(sparkConf: SparkConf) {
  /**
   * Spark session
   */
  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
}

object DataMiningEngine {

  private val logger: Logger = Logger(this.getClass)

  def apply(appName: String, sparkMaster: String): DataMiningEngine = {
    val sparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster(sparkMaster)
      .set("spark.driver.allowMultipleContexts", "false")
      .set("spark.ui.enabled", "false")

    val dme = new DataMiningEngine(sparkConf)
    logger.info("Data Mining Engine is up and running!")
    dme
  }

}
