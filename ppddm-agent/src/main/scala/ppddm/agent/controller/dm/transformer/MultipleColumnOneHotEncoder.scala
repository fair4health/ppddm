package ppddm.agent.controller.dm.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols}
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

/**
 * An OneHotEncoder-like encoder that maps each distinct value in string column(s) of labels
 * to ML column(s) and removes the input column. Empty values are just ignored, hence not
 * inserted as a new column. The column name format is {inputCol}_{columnValue},
 * e.g. gender_male, gender_female.
 *
 * @param uid
 */
class MultipleColumnOneHotEncoder(override val uid: String) extends Transformer with HasInputCol with HasInputCols {

  def this() = this(Identifiable.randomUID("multipleColumnOneHotEncoder"))

  override def copy(extra: ParamMap): MultipleColumnOneHotEncoder = defaultCopy(extra)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val inputColNames = getInCols()

    var result = dataset

    for (elem <- inputColNames) {
      val distinctValues = dataset.select(elem).distinct().collect()

      distinctValues.foreach( v =>
        if (v.getString(0) != null) {
          val colName = elem + "_" + v.getString(0)
          result = result.withColumn(colName, when(col(elem).equalTo(v.getString(0)), 1.0).otherwise(0.0))
        }
      )

      result = result.drop(elem)
    }

    result.toDF()
  }

  override def transformSchema(schema: StructType): StructType = {schema}

  /** Returns the input and output column names corresponding in pair. */
  private def getInCols(): Array[String] = {
    if (isSet(inputCol)) {
      Array($(inputCol))
    } else {
      $(inputCols)
    }
  }


}
