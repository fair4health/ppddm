package ppddm.agent.controller.dm

import com.typesafe.scalalogging.Logger
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame
import ppddm.agent.controller.prepare.{DataPreparationController, DataStoreManager}
import ppddm.core.rest.model.VariableType

/**
 * This object handles the preparation of data for a data mining algorithm
 */
object DataAnalysisManager {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Perform data analysis on the DataFrame previously saved with the given dataset_id
   * @param dataset_id The id of DataFrame saved in the data store previously
   * @return Updated DataFrame containing "features" and "label" column
   */
  def performDataAnalysis(dataset_id: String): DataFrame = {
    logger.debug("Performing data analysis...")

    // Retrieve the DataFrame object with the given dataset_id previously saved in the data store
    val df = DataStoreManager.getDF(DataStoreManager.getDatasetPath(dataset_id))

    if(df.isDefined) { // If DataFrame exists, then continue
      var dataFrame = df.get

      // Retrieve the previously saved DataPreparationResult object including variable information
      val dataPreperationResult = DataPreparationController.getDataSourceStatistics(dataset_id)

      if (dataPreperationResult.isDefined) {
        // TODO handle imbalanced data here: either remove from balanced or inject synthetic.
        // TODO if you don't want to do this, arrange threshold in classification
        // TODO however, this cannot be the case always. For example, in cancer case, if %98 is not cancer, %2 is cancer, synthetic or removing would not be meaningful

        // Find independent and dependent variables
        // When there is no dependent variable, variable_type can be empty. Therefore, treat these variables as independent
        val independentVariables = dataPreperationResult.get.agent_data_statistics.variable_statistics.filter( v =>
          v.variable.variable_type.isEmpty || v.variable.variable_type.get == VariableType.INDEPENDENT)

        // There can only be one dependent variable, but filter returns a list, so name it as list here.
        val dependentVariables = dataPreperationResult.get.agent_data_statistics.variable_statistics.filter( v =>
          v.variable.variable_type.isDefined && v.variable.variable_type.get == VariableType.DEPENDENT)

        // Introduce independent variables as Vector in "features" column
        dataFrame = new VectorAssembler()
          .setInputCols(independentVariables.map(iv => iv.variable.name).toArray) // columns that need to added to feature column
          .setOutputCol("features")
          .transform(dataFrame)

        // Introduce the dependent variable
        if (!dependentVariables.isEmpty) {
          dataFrame = new StringIndexer()
            .setInputCol(dependentVariables.head.variable.name)
            .setOutputCol("label")
            .fit(dataFrame).transform(dataFrame)
        }

        // TODO handle categorical variables

        // TODO handle null values

        // TODO handle feature scaling (here or somewhere else?)

        // TODO handle others

        logger.debug("Data analysis has been performed. Returning updated data frame...")
        dataFrame
      } else {
        df.get
      }
    } else { // Otherwise return empty DataFrame
      df.get
    }
  }
}
