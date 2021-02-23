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
    val fields = featureset.variables
      .map(variable =>
        StructField(
          removeInvalidChars(variable.name),
          if (variable.variable_data_type == VariableDataType.NUMERIC) DoubleType else StringType
        )
      )
    StructType(Seq(StructField("pid", StringType, nullable = false)) ++ fields)
  }

  /**
   * Removes invalid characters that prevent the recording and filtering of the Parquet files from being done correctly.
   *
   * @param value
   * @return
   */
  def removeInvalidChars(value: String): String = {
    value.trim.replaceAll("[\\s\\`\\*{}\\[\\]()>#\\+:\\~'%\\^&@<\\?;,\\\"!\\$=\\|\\.]", "")
  }
}
