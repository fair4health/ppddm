package ppddm.core.store

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
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
    //df.sparkSession.catalog.refreshByPath(path)
    // A workaround from https://forums.databricks.com/questions/21830/spark-how-to-simultaneously-read-from-and-write-to.html
    // df.cache().show(1)
    // df.write.format("parquet").mode(SaveMode.Overwrite).save(path)
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

  /**
   *
   *
   * @param path
   * @return
   */
  def deleteDirectoryAsync(path: String, sleep: Long = 1000): Future[Boolean] = {
    Future {
      Thread.sleep(sleep)
      deleteDirectory(path)
    }
  }

}

object DataStoreManager extends DataStoreManager {

}
