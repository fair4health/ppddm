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
            val valueDistribution: Option[Seq[ValueCount]] = calculateDistinctValuesCategorical(dataFrame, field.name, numberOfRecords)
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
            val valueDistribution: Option[Seq[ValueCount]] = calculateDistinctValuesNumeric(dataFrame, field.name, numberOfRecords)
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
  private def calculateNullPercentage(dataFrame: DataFrame, fieldName: String, numberOfRecords: Long): Option[Double] = {
    Try(
      (dataFrame.filter(fieldName + " is null").count() / numberOfRecords.toDouble) * 100
    ).toOption
  }

  /**
   * Calculates the number of records for each distinct value in a numeric column
   * @param dataFrame
   * @param fieldName
   * @return
   */
  private def calculateDistinctValuesNumeric(dataFrame: DataFrame, fieldName: String, numberOfRecords: Long): Option[Seq[ValueCount]] = {
    Try {
      val output = dataFrame.select(fieldName).groupBy(fieldName).count().collect().toSeq.map(x =>
        ValueCount(x.getDouble(0).toString, x.getLong(1), (x.getLong(1) / numberOfRecords.toDouble) * 100))
      reduceValueCountStatistics(output, numberOfRecords)
    }.toOption
  }

  /**
   * Calculates the number of records for each distinct value in a categorical column
   * @param dataFrame
   * @param fieldName
   * @return
   */
  private def calculateDistinctValuesCategorical(dataFrame: DataFrame, fieldName: String, numberOfRecords: Long): Option[Seq[ValueCount]] = {
    Try {
      val output = dataFrame.select(fieldName).groupBy(fieldName).count().collect().toSeq.map(x =>
        ValueCount(x.getString(0), x.getLong(1), (x.getLong(1) / numberOfRecords.toDouble) * 100))
      reduceValueCountStatistics(output, numberOfRecords)
    }.toOption
  }

  /**
   * Reduces the total number statistics to 5 items if there are more than 4 items in the list
   * @param statistics
   * @param numberOfRecords
   * @return
   */
  private def reduceValueCountStatistics(statistics: Seq[ValueCount], numberOfRecords: Long): Seq[ValueCount] = {
    if (statistics.size > 4) {
      // Sort the output in descending order and take the first 4 items
      val head = statistics.sortWith((a, b) => a.count > b.count).take(4)
      // Calculate the total count of first 4 items
      val countOfHeadItems = head.map(_.count).reduceLeft(_ + _).toInt
      // Calculate the total percentage of first 4 items
      val percentageOfHeadItems = head.map(_.percentage).reduceLeft(_ + _)
      // Add a 5th "others" item for the rest
      head :+ ValueCount("Others", numberOfRecords - countOfHeadItems, 100 - percentageOfHeadItems)
    } else {
      statistics
    }
  }
}
