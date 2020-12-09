package ppddm.agent.controller

import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import ppddm.agent.SparkSessionWrapper
import ppddm.agent.controller.prepare.StatisticsController
import ppddm.core.rest.model.{AgentDataStatistics, Variable, VariableStatistics}

@RunWith(classOf[JUnitRunner])
class StatisticsControllerTest extends Specification with SparkSessionWrapper {
  import sparkSession.implicits._

  /**
   * Create a dataframe with "strings1", "strings2" and "numbers" columns
        Table:
             |strings1|strings2|numbers|
             |-------------------------|
             |str1    |str1    |1      |
             |null    |str2    |2      |
             |null    |str3    |3      |
             |str4    |null    |4      |
   */
  lazy val tuples: Seq[(String, String, Double)] = Seq(("str1", "str1", 1), (null, "str2", 2), (null, "str3", 3), ("str4", null, 4))
  lazy val dataFrame: DataFrame = tuples.toDF("strings1", "strings2", "numbers")
  lazy val variables: Seq[Variable] = Seq(
    Variable(
      name = "strings1",
      description = None,
      fhir_query = "",
      fhir_path = "",
      variable_data_type = "categorical",
      variable_type = "independent"),
    Variable(
      name = "strings2",
      description = None,
      fhir_query = "",
      fhir_path = "",
      variable_data_type = "categorical",
      variable_type = "independent"),
    Variable(
      name = "numbers",
      description = None,
      fhir_query = "",
      fhir_path = "",
      variable_data_type = "numeric",
      variable_type = "independent")
  )

  lazy val agentDataStatistics: AgentDataStatistics = StatisticsController.calculateStatistics(dataFrame, variables)

  sequential

  "Statistics Controller" should {
    "return number of records correctly" in {
      agentDataStatistics.number_of_records shouldEqual 4
    }

    "calculate null percentages" in {
      val string1VariableStatistics: VariableStatistics = agentDataStatistics.variable_statistics.find(_.variable.name == "strings1").get
      string1VariableStatistics.null_percentage.get shouldEqual 50.0 // 50 percent of records for "strings1" column is null

      val string2VariableStatistics: VariableStatistics = agentDataStatistics.variable_statistics.find(_.variable.name == "strings2").get
      string2VariableStatistics.null_percentage.get shouldEqual 25.0 // 25 percent of records for "strings2" column is null

      val numericVariableStatistics: VariableStatistics = agentDataStatistics.variable_statistics.find(_.variable.name == "numbers").get
      numericVariableStatistics.null_percentage.get shouldEqual 0 // 0 percent of records for "numbers" column is null
    }

    "calculate min-max values for numeric fields" in {
      val numericVariableStatistics: VariableStatistics = agentDataStatistics.variable_statistics.find(_.variable.name == "numbers").get
      numericVariableStatistics.min_value.get shouldEqual 1 // min_value in the "numbers" column should be 1
      numericVariableStatistics.max_value.get shouldEqual 4 // max_value in the "numbers" column should be 4
    }
  }
}
