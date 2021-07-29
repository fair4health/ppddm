package ppddm.agent.controller.prepare

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}
import ppddm.core.rest.model.{AgentDataStatistics, DataType, Parameter, ValueCount, Variable, VariableStatistics}

import scala.util.Try

/**
 * Handles the calculation of statistics on the prepared data.
 */
object StatisticsController {

  /**
   * Calculate the statistics for the variables using the data of the dataFrame
   *
   * @param variables The variables for whom the statistics will be calculated
   * @param dataFrame The data to be used for statistics calculation
   * @return
   */
  def calculateStatistics(dataFrame: DataFrame, variables: Seq[Variable]): AgentDataStatistics = {
    // Get the number of records
    val numberOfRecords = dataFrame.count()
    // Init the final statistics list
    var variableStatisticsList: Seq[VariableStatistics] = Seq.empty[VariableStatistics]

    variables.foreach { variable =>
      val fieldOption = dataFrame.schema.find(s => s.name == variable.getMLValidName)
      if (fieldOption.isDefined) {
        fieldOption.get match {
          case field if field.dataType == StringType => // String type
            // Calculate the percentage of null values
            val nullPercentage: Option[Double] = calculateNullPercentage(dataFrame, field.name, numberOfRecords)
            // Calculate number of occurrences in each column
            val valueDistribution: Option[Seq[ValueCount]] = calculateDistinctValuesCategorical(dataFrame, field.name)
            // Create VariableStatistics obj
            val variableStatistics: VariableStatistics = VariableStatistics(variable, None, None, nullPercentage, valueDistribution)
            // Append to the statistics list
            variableStatisticsList = variableStatisticsList :+ variableStatistics
          case field if field.dataType == DoubleType => // Double type
            // Find the min max values. Tuple in the format: (min_value, max_value)
            val min_max: (Option[Double], Option[Double]) = getMinMax(dataFrame, field.name)
            // Calculate the percentage of null values
            val nullPercentage: Option[Double] = calculateNullPercentage(dataFrame, field.name, numberOfRecords)
            // Calculate number of occurrences in each column
            val valueDistribution: Option[Seq[ValueCount]] = calculateDistinctValuesNumeric(dataFrame, field.name)
            // Append to the statistics list
            val variableStatistics: VariableStatistics = VariableStatistics(variable, min_max._1, min_max._2, nullPercentage, valueDistribution)
            variableStatisticsList = variableStatisticsList :+ variableStatistics
          case _ => None
        }
      }
    }

    AgentDataStatistics(numberOfRecords, variableStatisticsList)
  }

  /**
   * Finds min and max values in the fields of Double types.
   *
   * @param dataFrame
   * @param fieldName
   * @return Tuple as (Option(min_value), Option(max_value))
   */
  private def getMinMax(dataFrame: DataFrame, fieldName: String): (Option[Double], Option[Double]) = {
    val min_max = dataFrame.agg(min(fieldName), max(fieldName)).head()

    val min_value: Option[Double] = Try(min_max.getDouble(0)).toOption
    val max_value: Option[Double] = Try(min_max.getDouble(1)).toOption

    (min_value, max_value)
  }

  /**
   * Calculates the percentage of null values.
   *
   * @param dataFrame
   * @param fieldName
   * @param numberOfRecords
   * @return
   */
  private def calculateNullPercentage(dataFrame: DataFrame, fieldName: String, numberOfRecords: Double): Option[Double] = {
    Try(
      (dataFrame.filter(fieldName + " is null").count() / numberOfRecords) * 100
    ).toOption
  }

  /**
   * Calculates the number of records for each distinct value in a numeric column
   * @param dataFrame
   * @param fieldName
   * @return
   */
  private def calculateDistinctValuesNumeric(dataFrame: DataFrame, fieldName: String): Option[Seq[ValueCount]] = {
    Try(
      dataFrame.select(fieldName).groupBy(fieldName).count().collect().map(x =>
        ValueCount(x.getDouble(0).toString, x.getLong(1).toInt)).toSeq
    ).toOption
  }

  /**
   * Calculates the number of records for each distinct value in a categorical column
   * @param dataFrame
   * @param fieldName
   * @return
   */
  private def calculateDistinctValuesCategorical(dataFrame: DataFrame, fieldName: String): Option[Seq[ValueCount]] = {
    Try(
      dataFrame.select(fieldName).groupBy(fieldName).count().collect().map(x =>
        ValueCount(x.getString(0), x.getLong(1).toInt)).toSeq
    ).toOption
  }
}
