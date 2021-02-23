package ppddm.agent.controller.dm

import com.typesafe.scalalogging.Logger
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.{MinMaxScaler, OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import ppddm.core.ai.transformer.{AgeTransformer, MultipleColumnOneHotEncoder}
import ppddm.agent.controller.prepare.DataPreparationController
import ppddm.core.rest.model.{DataMiningException, VariableDataType, VariableType}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * This object handles the exploratory data analysis (EDA) to prepare the data for Spark's machine learning algorithms.
 */
object DataAnalysisManager {
  private val logger: Logger = Logger(this.getClass)

  /**
   * Perform data analysis on the DataFrame previously saved with the given dataset_id which was prepared by the DataPreparationController.
   *
   * @param dataset_id The id of the Dataset prepared and stored in the data store previously
   * @param dataFrame The original DataFrame retrieved from DataStore
   * @return Array of "PipelineStage"s of several Feature Transformers like StringIndexer, OneHotEncoder, Vector Assembler which create "features" and "label" columns for machine learning algorithms
   */
  def performDataAnalysis(dataset_id: String, dataFrame: DataFrame): Future[Array[PipelineStage]] = {
    Future {
      logger.debug("Performing data analysis...")

      // Retrieve the previously saved DataPreparationResult object including variable information
      val dataPreperationResultOption = DataPreparationController.getDataSourceStatistics(dataset_id)
      if (dataPreperationResultOption.isEmpty) {
        val msg = s"The data source statistics (DataPreparationResult) for the Dataset with id:${dataset_id} on which Data Mining algorithms will be executed does not exist. This should not have happened!!"
        logger.error(msg)
        throw DataMiningException(msg)
      }
      val dataPreparationResult = dataPreperationResultOption.get

      var pipelineStages = new ListBuffer[PipelineStage]()

      // TODO handle imbalanced data here: either remove from balanced or inject synthetic.
      // TODO if you don't want to do this, arrange threshold in classification
      // TODO however, this cannot be the case always. For example, in cancer case, if %98 is not cancer, %2 is cancer, synthetic or removing would not be meaningful

      // TODO handle null values
      // TODO consider dropping columns with large number of missing values
      // TODO consider removing rows with a null value for an important variable

      // TODO consider dropping columns in which all the records have the same value

      /**
       * Handle categorical variables
       */
      logger.debug("Handling categorical variables...")

      // Find categorical variables and their column names
      val categoricalVariables = dataPreparationResult.agent_data_statistics.variable_statistics.filter( v =>
        v.variable.variable_data_type == VariableDataType.CATEGORICAL)
      val categoricalColumns = categoricalVariables.map(cv => cv.variable.getMLValidName)

      // For string type input data, first we need to encode categorical features into numbers using StringIndexer first
      val stringIndexerSeq = categoricalVariables.map(v => {
        new StringIndexer()
          .setInputCol(v.variable.getMLValidName)
          .setOutputCol(s"${v.variable.getMLValidName}_INDEX")
          .setStringOrderType("alphabetAsc") // options are "frequencyDesc", "frequencyAsc", "alphabetDesc", "alphabetAsc"
          .setHandleInvalid("keep") // options are "keep", "error" or "skip". "keep" puts unseen labels in a special additional bucket, at index numLabels
      })
      stringIndexerSeq.foreach(i => pipelineStages += i) // Now, DataFrame contains new columns with "_INDEX" at the end of column name

      // After all categorical values are in numeric format, apply OneHotEncoder to introduce dummy variables
      val encoder = new OneHotEncoder()
        .setInputCols(categoricalVariables.map(cv => s"${cv.variable.getMLValidName}_INDEX").toArray)
        .setOutputCols(categoricalVariables.map(cv => s"${cv.variable.getMLValidName}_VEC").toArray)
      pipelineStages += encoder // Now, DataFrame contains new columns with "_VEC" at the end of column name

      /**
       * Create "features" and "label" columns
       */
      logger.debug("Introducing new \"features\" and \"label\" columns...")

      // Find independent and dependent variables
      val independentVariables = dataPreparationResult.agent_data_statistics.variable_statistics
        .filter(_.variable.variable_type == VariableType.INDEPENDENT)

      // There can only be one dependent variable, but filter returns a list, so name it as list here.
      val dependentVariableOption = dataPreparationResult.agent_data_statistics.variable_statistics
        .find(_.variable.variable_type == VariableType.DEPENDENT)

      // Introduce independent variables as Vector in "nonScaledFeatures" column. We will later convert it to "features" column.
      val vectorAssembler = new VectorAssembler()
        .setInputCols(independentVariables.map(iv => if (categoricalColumns.contains(iv.variable.getMLValidName)) s"${iv.variable.getMLValidName}_VEC" else iv.variable.getMLValidName).toArray) // columns that need to added to feature column
        .setOutputCol("nonScaledFeatures")
      pipelineStages += vectorAssembler

      // Introduce the dependent variable
      if (dependentVariableOption.nonEmpty) {
        val labelStringIndexer = new StringIndexer()
          .setInputCol(dependentVariableOption.get.variable.getMLValidName)
          .setOutputCol("label")
          .setHandleInvalid("keep") // options are "keep", "error" or "skip". "keep" puts unseen labels in a special additional bucket, at index numLabels
        pipelineStages += labelStringIndexer
      }

      /**
       * Handle feature scaling
       */
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

      val scaler = new MinMaxScaler() // Scale features to [0,1]
        .setInputCol("nonScaledFeatures")
        .setOutputCol("features")
      pipelineStages += scaler

      // TODO handle others

      logger.debug("Exploratory Data Analysis has been performed...")
      pipelineStages.toArray
    }
  }

  /**
   * Applies MultipleColumnOneHotEncoder and AgeTransformer on a data frame and returns the resulting data frame
   * @param dataFrame
   * @return
   */
  def performCategoricalTransformations(dataFrame: DataFrame): DataFrame = {
    // Find categorical variables which are in StringType
    val inputCols = dataFrame.schema.filter(s => s.dataType == StringType && !s.name.equals("pid")).map(_.name)

    // Apply MultipleColumnOneHotEncoder
    val updateDataFrame = new MultipleColumnOneHotEncoder().setInputCols(inputCols.toArray).transform(dataFrame)

    // If age column exists in the data frame, apply AgeTransformer. Otherwise return the data frame.
    if (updateDataFrame.schema.exists(_.name.toLowerCase == "age"))
      new AgeTransformer().setInputCol("age").transform(updateDataFrame)
    else updateDataFrame
  }
}
