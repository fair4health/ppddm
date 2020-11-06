package ppddm.agent

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  lazy implicit val sparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("ppddm-agent-test")
      .getOrCreate()
  }
}
