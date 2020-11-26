package ppddm.agent.controller.dm.algorithm.arl

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.sql.DataFrame
import ppddm.core.rest.model.{ARLModel, Agent, Algorithm, AlgorithmParameterName}
import org.apache.spark.sql.functions._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

case class FPGrowthPPDDM(override val agent: Agent, override val algorithm: Algorithm) extends ARLAlgorithm {

  import sparkSession.implicits._

  def execute(frequentItemDataFrame: DataFrame): Future[ARLModel] = {
    logger.debug(s"## Start executing ${algorithm.name} ##")

    Future {
      logger.debug("Creating the new data frame for the frequent items...")
      val schema = frequentItemDataFrame.schema.map(_.name)

      // Since VectorAssembler does not work with String type, we do several steps manually
      // First, change 0.0 values with -1 and other values with the index of column name in the schema
      var newDataFrame = frequentItemDataFrame
      for ((field, index) <- schema.zipWithIndex) {
        newDataFrame = newDataFrame.withColumn(s"${field}",
          when(col(s"${field}").equalTo(0.0), -1).otherwise(index))
      }

      // Then, put all the values into single vector
      val assembler = new VectorAssembler()
        .setInputCols(schema.toArray)
        .setOutputCol("numberVector")
      newDataFrame = assembler.transform(newDataFrame)

      // Take this vector, convert to array, remove -1 values, and replace index values with item names
      val vectorToArray = udf( (xs: org.apache.spark.ml.linalg.Vector) => xs.toArray.filter(_ > -1).map( i => schema(i.toInt)) )
      newDataFrame = newDataFrame.withColumn("items", vectorToArray($"numberVector"))

      logger.debug("Creating the FPGrowth object...")
      val minSupport = 0 // We don't use min support here. We use it in the PPDDM Manager for eliminating infrequent items globally. Hence set it to zero here
      var minConfidence = 0.5 // Default value
      algorithm.parameters.foreach( p => {
        p.name match {
          // case AlgorithmParameterName.MIN_SUPPORT => See the comment above
          case AlgorithmParameterName.MIN_CONFIDENCE => minConfidence = p.getValueAsDoubleArray().head
          case _ => None
          // Add others here
        }
      })
      val fpGrowth = new FPGrowth().setItemsCol("items").setMinSupport(minSupport).setMinConfidence(minConfidence)

      logger.debug("Fitting the model...")
      val pipeline = new Pipeline().setStages(Array(fpGrowth))
      val model = pipeline.fit(newDataFrame)

      // Display frequent itemsets.
      model.stages.last.asInstanceOf[FPGrowthModel].freqItemsets.show()
      // Display generated association rules.
      model.stages.last.asInstanceOf[FPGrowthModel].associationRules.show()

      logger.debug(s"## Finish executing ${algorithm.name} ##")

      ARLModel(algorithm, agent, toString(model))
    }
  }
}
