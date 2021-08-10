package ppddm.agent.controller.dm

import com.typesafe.scalalogging.Logger
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.{Imputer, MinMaxScaler, OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import ppddm.agent.controller.prepare.DataPreparationController
import ppddm.core.ai.transformer.{AgeTransformer, MultipleColumnOneHotEncoder}
import ppddm.core.rest.model.{Algorithm, AlgorithmParameterName, DataMiningException, VariableDataType, VariableType}

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
   * @param dataFrame  The original DataFrame retrieved from DataStore
   * @param algorithm  The algorithm
   * @return Array of "PipelineStage"s of several Feature Transformers like StringIndexer, OneHotEncoder, Vector Assembler which create "features" and "label" columns for machine learning algorithms
   */
  def performDataAnalysis(dataset_id: String, dataFrame: DataFrame, algorithm: Algorithm): Future[Array[PipelineStage]] = {
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

      val pipelineStages = new ListBuffer[PipelineStage]()

      // TODO handle imbalanced data here: either remove from balanced or inject synthetic.
      // TODO if you don't want to do this, arrange threshold in classification
      // TODO however, this cannot be the case always. For example, in cancer case, if %98 is not cancer, %2 is cancer, synthetic or removing would not be meaningful

      logger.debug("Building the pipeline...")

      // TODO consider dropping columns with large number of missing values
      // TODO consider removing rows with a null value for an important variable

      // TODO consider dropping columns in which all the records have the same value

      // options are "keep", "error" or "skip". "keep" puts unseen labels in a special additional bucket, at index numLabels
      val handleInvalid = algorithm.parameters.find(_.name == AlgorithmParameterName.HANDLE_INVALID).map(_.value).getOrElse("keep")

      logger.debug("The handleInvalid value is {}", handleInvalid)

      // Find numeric variables and their column names
      val numericVariables = dataPreparationResult.agent_data_statistics.variable_statistics.filter(v =>
        v.variable.variable_data_type == VariableDataType.NUMERIC)
      val numericColumns = numericVariables.map(cv => cv.variable.getMLValidName)

      // Find categorical variables and their column names
      val categoricalVariables = dataPreparationResult.agent_data_statistics.variable_statistics.filter(v =>
        v.variable.variable_data_type == VariableDataType.CATEGORICAL)
      val categoricalColumns = categoricalVariables.map(cv => cv.variable.getMLValidName)

      if (handleInvalid == "keep") { // Apply the imputation to numeric columns only if the request is to "keep" the null/invalid cells.
        // options are "mean" and "median".
        val imputationStrategy = algorithm.parameters.find(_.name == AlgorithmParameterName.IMPUTATION_STRATEGY).map(_.value).getOrElse("median")
        val imputer = new Imputer()
          .setInputCols(numericColumns.toArray)
          .setOutputCols(numericColumns.map(n => s"${n}_NUM").toArray) // Now, DataFrame contains new columns with "_NUM" at the end of column name, for numeric columns
          .setStrategy(imputationStrategy)
        pipelineStages += imputer
        logger.debug("The Imputer with the strategy:{} is added to the Pipeline", imputationStrategy)
      }

      /**
       * Handle categorical variables
       */
      logger.debug("Handling categorical variables...")

      // For string type input data, first we need to encode categorical features into numbers using StringIndexer first
      val stringIndexerSeq = categoricalColumns.map(columnName => {
        new StringIndexer()
          .setInputCol(columnName)
          .setOutputCol(s"${columnName}_INDEX")
          .setStringOrderType("alphabetAsc") // options are "frequencyDesc", "frequencyAsc", "alphabetDesc", "alphabetAsc"
          .setHandleInvalid(handleInvalid)
      })
      stringIndexerSeq.foreach(i => pipelineStages += i) // Now, DataFrame contains new columns with "_INDEX" at the end of column name, for categorical columns
      logger.debug("StringIndexers are added to the Pipeline for {} categorical feature columns.", categoricalColumns.size)

      // After all categorical values are in numeric format, apply OneHotEncoder to introduce dummy variables
      val encoder = new OneHotEncoder()
        .setInputCols(categoricalVariables.map(cv => s"${cv.variable.getMLValidName}_INDEX").toArray)
        .setOutputCols(categoricalVariables.map(cv => s"${cv.variable.getMLValidName}_VEC").toArray)
      pipelineStages += encoder // Now, DataFrame contains new columns with "_VEC" at the end of column name
      logger.debug("OneHotEncoder is added to the Pipeline to create a single _VEC column out of {} _INDEX columns.", categoricalColumns.size)

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

      // Decide the column names to be put into the "features" column
      val inputFeatureColumns = independentVariables.map { iv =>
        if (iv.variable.variable_data_type == VariableDataType.CATEGORICAL) {
          s"${iv.variable.getMLValidName}_VEC"
        } else {
          if (handleInvalid == "keep") {
            s"${iv.variable.getMLValidName}_NUM"
          } else {
            iv.variable.getMLValidName
          }
        }
      }
      logger.debug("The input columns for the \"features\" column are: {}", inputFeatureColumns.mkString(","))

      // Introduce independent variables as Vector in "nonScaledFeatures" column. We will later convert it to "features" column.
      val vectorAssembler = new VectorAssembler()
        .setInputCols(inputFeatureColumns.toArray) // columns that need to be added to feature column
        .setOutputCol("nonScaledFeatures")
        .setHandleInvalid(handleInvalid)
      pipelineStages += vectorAssembler
      logger.debug("VectorAssembler is added to the Pipeline")

      // Introduce the dependent variable
      if (dependentVariableOption.nonEmpty) {
        val labelColumnName =
          if (dependentVariableOption.get.variable.variable_data_type == VariableDataType.CATEGORICAL) dependentVariableOption.get.variable.getMLValidName
          else {
            if (handleInvalid == "keep") s"${dependentVariableOption.get.variable.getMLValidName}_NUM"
            else dependentVariableOption.get.variable.getMLValidName
          }
        val labelStringIndexer = new StringIndexer()
          .setInputCol(labelColumnName)
          .setOutputCol("label")
          .setHandleInvalid(handleInvalid)
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
       * val sparseToDense = udf((v : SparseVector) => v.toDense)
       * dataFrame = dataFrame.withColumn("notScaledFeatures", sparseToDense($"notScaledFeatures"))
       * val scaler = new StandardScaler()
       * .setInputCol("notScaledFeatures")
       * .setOutputCol("features")
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
   *
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
