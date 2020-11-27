package ppddm.agent.controller.dm.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.{col, udf, when}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

/**
 * A feature transformer that merges multiple string columns into a vector column.
 * It performs the same operation as `VectorAssembler`, but for string type which is not
 * supported by `VectorAssembler`.
 *
 */
class StringVectorAssembler(override val uid: String) extends Transformer with HasInputCols with HasOutputCol {

  def this() = this(Identifiable.randomUID("stringVectorAssembler"))

  override def copy(extra: ParamMap): StringVectorAssembler = defaultCopy(extra)

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    var result = dataset

    // First, change 0.0 values with -1 and other values with the index of column name in the schema
    for ((field, index) <- $(inputCols).zipWithIndex) {
      result = result.withColumn(s"${field}", when(col(s"${field}").equalTo(0.0), -1).otherwise(index))
    }

    // Then, put all the values into single vector
    val assembler = new VectorAssembler()
      .setInputCols($(inputCols))
      .setOutputCol("numberVector")
    result = assembler.transform(result)

    // Take this vector, convert to array, remove -1 values, and replace index values with item names
    val vectorToArray = udf( (xs: org.apache.spark.ml.linalg.Vector) => xs.toArray.filter(_ > -1).map( i => $(inputCols)(i.toInt)) )
    result = result.withColumn($(outputCol), vectorToArray(result("numberVector")))

    // Finally, remove the temporarily added numberVector column and return the result.
    result.drop("numberVector").toDF()
  }

  override def transformSchema(schema: StructType): StructType = {schema}
}
