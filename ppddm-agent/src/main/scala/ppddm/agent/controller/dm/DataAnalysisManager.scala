package ppddm.agent.controller.dm

import com.typesafe.scalalogging.Logger
import org.apache.spark.ml.feature.{MinMaxScaler, OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import ppddm.agent.Agent
import ppddm.agent.controller.prepare.DataPreparationController
import ppddm.agent.exception.DataMiningException
import ppddm.agent.store.DataStoreManager
import ppddm.core.rest.model.{VariableDataType, VariableType}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * This object handles the exploratory data analysis (EDA) to prepare the data for Spark's machine learning algorithms.
 */
object DataAnalysisManager {

  private val logger: Logger = Logger(this.getClass)
  private val sparkSession: SparkSession = Agent.dataMiningEngine.sparkSession

  import sparkSession.implicits._

  /**
   * Perform data analysis on the DataFrame previously saved with the given dataset_id which was prepared by the DataPreparationController.
   *
   * @param dataset_id The id of the Dataset prepared and stored in the data store previously
   * @return A new DataFrame containing "features" and "label" column which is ready for machine learning algorithms
   */
  def performDataAnalysis(dataset_id: String): Future[DataFrame] = {
    Future {
      logger.debug("Performing data analysis using the Dataset with id:{}...", dataset_id)

      // Retrieve the DataFrame object with the given dataset_id previously saved in the data store
      val dataFrameOption = DataStoreManager.getDataFrame(DataStoreManager.getDatasetPath(dataset_id))
      if (dataFrameOption.isEmpty) {
        val msg = s"The Dataset with id:${dataset_id} on which Data Mining algorithms will be executed does not exist. This should not have happened!!"
        logger.error(msg)
        throw DataMiningException(msg)
      }

      // Retrieve the previously saved DataPreparationResult object including variable information
      val dataPreperationResultOption = DataPreparationController.getDataSourceStatistics(dataset_id)
      if (dataPreperationResultOption.isEmpty) {
        val msg = s"The data source statistics (DataPreparationResult) for the Dataset with id:${dataset_id} on which Data Mining algorithms will be executed does not exist. This should not have happened!!"
        logger.error(msg)
        throw DataMiningException(msg)
      }

      var dataFrame = dataFrameOption.get
      val dataPreparationResult = dataPreperationResultOption.get

      // TODO handle imbalanced data here: either remove from balanced or inject synthetic.
      // TODO if you don't want to do this, arrange threshold in classification
      // TODO however, this cannot be the case always. For example, in cancer case, if %98 is not cancer, %2 is cancer, synthetic or removing would not be meaningful

      // TODO handle null values
      // TODO consider dropping columns with large number of missing values
      // TODO consider removing rows with a null value for an important variable

      // TODO consider dropping columns in which all the records have the same value

      // ### Handle categorical variables ###
      logger.debug("Handling categorical variables...")

      // We need to introduce new columns while using OneHotEncoder to introduce dummy variables.
      // Hence keep the column names of the DataFrame in a variable.
      val dataFrameColumns: Array[String] = dataFrame.columns

      // Find categorical variables and their column names
      val categoricalVariables = dataPreparationResult.agent_data_statistics.variable_statistics.filter( v =>
        v.variable.variable_data_type == VariableDataType.CATEGORICAL)
      val categoricalColumns = categoricalVariables.map(cv => cv.variable.name)

      // For string type input data, first we need to encode categorical features into numbers using StringIndexer first
      categoricalVariables.foreach(v => {
        val indexer = new StringIndexer()
          .setInputCol(v.variable.name)
          .setOutputCol(s"${v.variable.name}_INDEX")
          .setHandleInvalid("keep") // options are "keep", "error" or "skip". "keep" puts unseen labels in a special additional bucket, at index numLabels
        dataFrame = indexer.fit(dataFrame).transform(dataFrame) // Now, DataFrame contains new columns with "_INDEX" at the end of column name
      })

      // After all categorical values are in numeric format, apply OneHotEncoder to introduce dummy variables
      val encoder = new OneHotEncoder()
        .setInputCols(categoricalVariables.map(cv => s"${cv.variable.name}_INDEX").toArray)
        .setOutputCols(categoricalVariables.map(cv => s"${cv.variable.name}_VEC").toArray)
      dataFrame = encoder.fit(dataFrame).transform(dataFrame) // Now, DataFrame contains new columns with "_VEC" at the end of column name

      // Put the encoded values to their original position so that dataFrame still has the same StructType as the initial one
      val newColumns: Array[String] = dataFrameColumns.map(column => if (categoricalColumns.contains(column)) s"${column}_VEC" else column)
      dataFrame = dataFrame.select(newColumns.head, newColumns.tail: _*) // Reposition columns
        .toDF(dataFrameColumns:_*) // Update their names
      logger.debug("Categorical variables have been handled...")

      // ### Create "features" and "label" columns ###
      logger.debug("Creating features and label columns...")

      // Find independent and dependent variables
      val independentVariables = dataPreparationResult.agent_data_statistics.variable_statistics
        .filter(_.variable.variable_type == VariableType.INDEPENDENT)

      // There can only be one dependent variable, but filter returns a list, so name it as list here.
      val dependentVariableOption = dataPreparationResult.agent_data_statistics.variable_statistics
        .find(_.variable.variable_type == VariableType.DEPENDENT)

      // Introduce independent variables as Vector in "nonScaledFeatures" column. We will later convert it to "features" column.
      dataFrame = new VectorAssembler()
        .setInputCols(independentVariables.map(iv => iv.variable.name).toArray) // columns that need to added to feature column
        .setOutputCol("nonScaledFeatures")
        .transform(dataFrame)

      // Introduce the dependent variable
      if (dependentVariableOption.nonEmpty) {
        dataFrame = new StringIndexer()
          .setInputCol(dependentVariableOption.get.variable.name)
          .setOutputCol("label")
          .fit(dataFrame).transform(dataFrame)
      }

      // ### Handle feature scaling ###
      logger.debug("Handling feature scaling...")

      /**
       * Keep this comment here, in case we change the scaler to StandardScaler.
       * When using StandardScaler, first convert the sparse vector in features column to a dense vector as a fail safe
       * Because, there is a caveat with standardization in spark. Unfortunately, standard scaler does not internally convert the sparse vector to a dense vector
       * Here is the code:
              val sparseToDense = udf((v : SparseVector) => v.toDense)
              dataFrame = dataFrame.withColumn("notScaledFeatures", sparseToDense($"notScaledFeatures"))
              val scaler = new StandardScaler()
                .setInputCol("notScaledFeatures")
                .setOutputCol("features")
       */

      val scaler = new MinMaxScaler()
        .setInputCol("nonScaledFeatures")
        .setOutputCol("features")

      dataFrame = scaler.fit(dataFrame).transform(dataFrame) // Scale features to [0,1]
        .drop("nonScaledFeatures") // Remove the column containing non-scaled features, because we don't need it anymore
      logger.debug("Features have been scaled...")

      logger.debug("Features and label columns have been created...")

      // TODO handle others

      logger.debug("Exploratory Data Analysis has been performed on Dataset with id:{}. Returning the machine-learning-ready DataFrame...", dataset_id)
      dataFrame
    }
  }
}
