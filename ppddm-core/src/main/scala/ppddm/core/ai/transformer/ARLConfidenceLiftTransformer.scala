package ppddm.core.ai.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
 * A special kind of Transformer which takes frequent items and total record count as input and
 * calculates the confidence and lift for the given dataset containing association rules in form
 * of antecedent => consequent.
 * The result is a DataFrame with four columns: antecedent, consequent, confidence, lift
 *
 */
class ARLConfidenceLiftTransformer(override val uid: String) extends Transformer {

  final val freqItems: Param[Seq[Row]] = new Param[Seq[Row]](this, "freqItems", "frequent items")
  final val totalRecordCount: Param[Long] = new Param[Long](this, "totalRecordCount", "total number of records")

  def this() = this(Identifiable.randomUID("arlConfidenceLiftTransformer"))

  def setFreqItems(value: Seq[Row]): this.type = set(freqItems, value)
  def setTotalRecordCount(value: Long): this.type = set(totalRecordCount, value)

  override def copy(extra: ParamMap): ARLConfidenceLiftTransformer = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    // Helper function to calculate confidence of an association rule. m1 corresponds to antecedent, while m2 corresponds to consequent
    val calculateConfidence = udf( (m1: Seq[String], m2: Seq[String]) => {
      // Confidence(M1 -> M2) is calculated as freq(M1 -> M2) / freq(M1)
      // Therefore, in the nominator, concatenate m1 and m2 (antecedent and consequent) and find its frequency
      // In denominator, find m1's (antecedent's) frequency
      val nominator = $(freqItems).filter(row => row.getSeq[String](0).sorted == (m1 ++ m2).sorted).head.getLong(1)
      val denominator = $(freqItems).filter(row => row.getSeq[String](0).sorted == m1.sorted).head.getLong(1)

      nominator.toDouble / denominator.toDouble
    })

    // Helper function to calculate lift of an association rule. m2 corresponds to consequent
    val calculateLift = udf ( (confidence: Double, m2: Seq[String]) => {
      // Lift(M1 -> M2) is calculated as confidence(M1 -> M2) / support(M2) where
      // Support (M) is calculated as freq(M) / total_count
      // Therefore, find the nominator of support and calculate the result of formula
      val nominatorOfSupport = $(freqItems).filter(row => row.getSeq[String](0).sorted == m2.sorted).head.getLong(1)

      confidence / (nominatorOfSupport.toDouble / $(totalRecordCount).toDouble)
    })

    // Add confidence and lift columns and return the result
    dataset.withColumn("confidence", calculateConfidence(col("antecedent"), col("consequent")))
           .withColumn("lift", calculateLift(col("confidence"), col("consequent")))
  }

  override def transformSchema(schema: StructType): StructType = {schema}

}
