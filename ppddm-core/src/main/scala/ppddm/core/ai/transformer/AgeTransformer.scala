package ppddm.core.ai.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

/**
 * A feature transformer that creates age group columns for the given age column.
 * The age groups are: 0-10, 10-20, 20-30, 30-40, 40-50, 50-60, 60-70, 70-80, 80+.
 *
 */
class AgeTransformer(override val uid: String) extends Transformer with HasInputCol {

  def this() = this(Identifiable.randomUID("ageTransformer"))

  override def copy(extra: ParamMap): AgeTransformer = defaultCopy(extra)

  def setInputCol(value: String): this.type = set(inputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    var result = dataset

    val ageGroups = Array(
      AgeGroup(0.0, 10.0, "0-10"),
      AgeGroup(10.0, 20.0, "10-20"),
      AgeGroup(20.0, 30.0, "20-30"),
      AgeGroup(30.0, 40.0, "30-40"),
      AgeGroup(40.0, 50.0, "40-50"),
      AgeGroup(50.0, 60.0, "50-60"),
      AgeGroup(60.0, 70.0, "60-70"),
      AgeGroup(70.0, 80.0, "70-80"),
      AgeGroup(80.0, 1000.0, "80+"))

    ageGroups.foreach( ageGroup => {
      val colName = $(inputCol) + "_" + ageGroup.display
      result = result.withColumn(colName, when(col($(inputCol)) >= ageGroup.min && col($(inputCol)) < ageGroup.max, 1.0).otherwise(0.0))
    })

    result.drop($(inputCol)).toDF()
  }

  override def transformSchema(schema: StructType): StructType = {schema}

  private final case class AgeGroup(min: Double,
                                    max: Double,
                                    display: String)
}
