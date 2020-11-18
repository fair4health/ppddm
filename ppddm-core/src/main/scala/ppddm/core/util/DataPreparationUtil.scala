package ppddm.core.util

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import ppddm.core.rest.model.{Featureset, VariableDataType}

object DataPreparationUtil {

  /**
   * Generate the schema for the DataFrame by using the Variable definitions of the Featureset
   *
   * @param featureset The Featureset
   * @return The schema of the resulting data in the format of StructType
   */
  def generateSchema(featureset: Featureset): StructType = {
    val fields = featureset.variables.get
      .map(variable =>
        StructField(
          variable.name /*.trim.replaceAll("\\s", "")*/ ,
          if (variable.variable_data_type == VariableDataType.NUMERIC) DoubleType else StringType
        )
      )
    StructType(Seq(StructField("pid", StringType, nullable = false)) ++ fields)
  }
}
