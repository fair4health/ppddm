package ppddm.agent.controller.dm.algorithm

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.ParamGridBuilder
import ppddm.core.rest.model.{Agent, Algorithm}

case class NaiveBayesPPDDM(override val agent: Agent, override val algorithm: Algorithm) extends DataMiningAlgorithm {

  override def getClassifierAndParamGrid(): (Array[PipelineStage], Array[ParamMap]) = {
    // Create the classifier
    val naiveBayesClassifier = new NaiveBayes()

    // Handle the paramGrid
    val paramGridBuilder = new ParamGridBuilder()
    algorithm.parameters.foreach( p => {
      p.name match {
        // No parameter for Naive Bayes
        case _ => None
        // Add others here
      }
    })
    val paramGrid = paramGridBuilder.build()

    // Return
    (Array(naiveBayesClassifier), paramGrid)
  }
}
