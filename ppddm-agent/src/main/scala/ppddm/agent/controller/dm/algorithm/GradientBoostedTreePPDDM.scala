package ppddm.agent.controller.dm.algorithm

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.ParamGridBuilder
import ppddm.core.rest.model.{Agent, Algorithm, AlgorithmParameterName}

private case class GradientBoostedTreePPDDM(override val agent: Agent, override val algorithm: Algorithm) extends DataMiningAlgorithm {

  override def getClassifierAndParamGrid(): (Array[PipelineStage], Array[ParamMap]) = {
    // Create the classifier
    val gbtClassifier = new GBTClassifier()

    // Handle the paramGrid
    val paramGridBuilder = new ParamGridBuilder()
    algorithm.parameters.foreach( p => {
      p.name match {
        case AlgorithmParameterName.MAX_DEPTH => paramGridBuilder.addGrid(gbtClassifier.maxDepth, p.getValueAsIntArray())
        case AlgorithmParameterName.MIN_INFO_GAIN => paramGridBuilder.addGrid(gbtClassifier.minInfoGain, p.getValueAsDoubleArray())
        case AlgorithmParameterName.FEATURE_SUBSET_STRATEGY => paramGridBuilder.addGrid(gbtClassifier.featureSubsetStrategy, p.getValueAsStringArray())
        case AlgorithmParameterName.MAX_ITER => paramGridBuilder.addGrid(gbtClassifier.maxIter, p.getValueAsIntArray())
        case _ => None
        // Add others here
      }
    })
    val paramGrid = paramGridBuilder.build()

    // Return
    (Array(gbtClassifier), paramGrid)
  }
}
