package ppddm.agent.controller.dm.algorithm

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.ParamGridBuilder
import ppddm.core.rest.model.{Agent, Algorithm, AlgorithmParameterName}

private case class DecisionTreePPDDM(override val agent: Agent, override val algorithm: Algorithm) extends DataMiningAlgorithm {

  override def getClassifierAndParamGrid(): (Array[PipelineStage], Array[ParamMap]) = {
    // Create the classifier
    val decisionTreeClassifier = new DecisionTreeClassifier()

    // Handle the paramGrid
    val paramGridBuilder = new ParamGridBuilder()
    algorithm.parameters.foreach( p => {
      p.name match {
        case AlgorithmParameterName.MAX_DEPTH => paramGridBuilder.addGrid(decisionTreeClassifier.maxDepth, p.getValueAsIntArray())
        case AlgorithmParameterName.MIN_INFO_GAIN => paramGridBuilder.addGrid(decisionTreeClassifier.minInfoGain, p.getValueAsDoubleArray())
        case AlgorithmParameterName.IMPURITY => paramGridBuilder.addGrid(decisionTreeClassifier.impurity, p.getValueAsStringArray())
        case _ => None
        // Add others here
      }
    })
    val paramGrid = paramGridBuilder.build()

    // Return
    (Array(decisionTreeClassifier), paramGrid)
  }
}
