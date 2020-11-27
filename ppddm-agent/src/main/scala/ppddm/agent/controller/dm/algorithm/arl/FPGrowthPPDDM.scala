package ppddm.agent.controller.dm.algorithm.arl

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.sql.DataFrame
import ppddm.agent.controller.dm.transformer.StringVectorAssembler
import ppddm.core.rest.model.{ARLModel, Agent, Algorithm, AlgorithmParameterName}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class FPGrowthPPDDM(override val agent: Agent, override val algorithm: Algorithm) extends ARLAlgorithm {

  def execute(frequentItemDataFrame: DataFrame): Future[ARLModel] = {
    logger.debug(s"## Start executing ${algorithm.name} ##")

    Future {
      logger.debug("Creating the items column...")
      val schema = frequentItemDataFrame.schema.map(_.name)
      val assembler = new StringVectorAssembler()
        .setInputCols(schema.toArray)
        .setOutputCol("items")
      val itemsDataFrame = assembler.transform(frequentItemDataFrame)

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
      val model = pipeline.fit(itemsDataFrame)

      // Display frequent itemsets.
      model.stages.last.asInstanceOf[FPGrowthModel].freqItemsets.show()
      // Display generated association rules.
      model.stages.last.asInstanceOf[FPGrowthModel].associationRules.show()

      logger.debug(s"## Finish executing ${algorithm.name} ##")

      ARLModel(algorithm, agent, toString(model))
    }
  }
}
