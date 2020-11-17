package ppddm.core.store

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.io.Directory
import scala.util.Try

trait DataStoreManager {

  final protected val BASE_STORE_DIR: String = "ppddm-store/"

  /**
   * Saves the DataFrame to the given file path
   *
   * @param path The filepath to save the DataFrame
   * @param df   The DataFrame to be saved
   * @return
   */
  def saveDataFrame(path: String, df: DataFrame): Unit = {
    df.write.parquet(path)
  }

  /**
   * Retrieves the DataFrame from the given file path
   *
   * @param path The filepath of the DataFrame
   * @return the DataFrame if it is found
   */
  def getDataFrame(path: String)(implicit sparkSession: SparkSession): Option[DataFrame] = {
    Try(sparkSession.read.parquet(path)).toOption
  }

  /**
   * Deletes the files under the given path recursively.
   *
   * @param path The path to the directory
   */
  def deleteDirectory(path: String): Boolean = {
    new Directory(new File(path)).deleteRecursively()
  }

}

object DataStoreManager extends DataStoreManager {

}
