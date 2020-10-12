package ppddm.agent.controller.dm

import com.typesafe.scalalogging.Logger
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame
import ppddm.agent.controller.prepare.DataPreparationController
import ppddm.agent.exception.DataMiningException
import ppddm.agent.store.DataStoreManager
import ppddm.core.rest.model.VariableType

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
        logger.error("msg")
        throw DataMiningException(msg)
      }

      var dataFrame = dataFrameOption.get
      val dataPreparationResult = dataPreperationResultOption.get

      // TODO handle imbalanced data here: either remove from balanced or inject synthetic.
      // TODO if you don't want to do this, arrange threshold in classification
      // TODO however, this cannot be the case always. For example, in cancer case, if %98 is not cancer, %2 is cancer, synthetic or removing would not be meaningful

      // Find independent and dependent variables
      val independentVariables = dataPreparationResult.agent_data_statistics.variable_statistics
        .filter(_.variable.variable_type == VariableType.INDEPENDENT)

      // There can only be one dependent variable, but filter returns a list, so name it as list here.
      val dependentVariableOption = dataPreparationResult.agent_data_statistics.variable_statistics
        .find(_.variable.variable_type == VariableType.DEPENDENT)

      // Introduce independent variables as Vector in "features" column
      dataFrame = new VectorAssembler()
        .setInputCols(independentVariables.map(iv => iv.variable.name).toArray) // columns that need to added to feature column
        .setOutputCol("features")
        .transform(dataFrame)

      // Introduce the dependent variable
      if (dependentVariableOption.nonEmpty) {
        dataFrame = new StringIndexer()
          .setInputCol(dependentVariableOption.get.variable.name)
          .setOutputCol("label")
          .fit(dataFrame).transform(dataFrame)
      }

      // TODO handle categorical variables

      // TODO handle null values

      // TODO handle feature scaling (here or somewhere else?)

      // TODO handle others

      logger.debug("Exploratory Data Analysis has been performed on Dataset with id:{}. Returning the machine-learning-ready DataFrame...", dataset_id)
      dataFrame
    }
  }
}
