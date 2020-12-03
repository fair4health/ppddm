package ppddm.agent.controller.dm.algorithm.classification

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.ParamGridBuilder
import ppddm.core.rest.model.{Agent, Algorithm, AlgorithmParameterName}

case class RandomForestPPDDM(override val agent: Agent, override val algorithm: Algorithm) extends ClassificationAlgorithm {

  override def getClassifierAndParamGrid(): (Array[PipelineStage], Array[ParamMap]) = {
    // Create the classifier
    val randomForestClassifier = new RandomForestClassifier()

    // Handle the paramGrid
    val paramGridBuilder = new ParamGridBuilder()
    algorithm.parameters.foreach( p => {
      p.name match {
        case AlgorithmParameterName.MAX_DEPTH => paramGridBuilder.addGrid(randomForestClassifier.maxDepth, p.getValueAsIntArray())
        case AlgorithmParameterName.MIN_INFO_GAIN => paramGridBuilder.addGrid(randomForestClassifier.minInfoGain, p.getValueAsDoubleArray())
        case AlgorithmParameterName.IMPURITY => paramGridBuilder.addGrid(randomForestClassifier.impurity, p.getValueAsStringArray())
        case AlgorithmParameterName.NUM_TREES => paramGridBuilder.addGrid(randomForestClassifier.numTrees, p.getValueAsIntArray())
        case AlgorithmParameterName.FEATURE_SUBSET_STRATEGY => paramGridBuilder.addGrid(randomForestClassifier.featureSubsetStrategy, p.getValueAsStringArray())
        case _ => None
        // Add others here
      }
    })
    val paramGrid = paramGridBuilder.build()

    // Return
    (Array(randomForestClassifier), paramGrid)
  }
}
