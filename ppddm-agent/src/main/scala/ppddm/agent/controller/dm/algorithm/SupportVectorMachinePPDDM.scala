package ppddm.agent.controller.dm.algorithm

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.ParamGridBuilder
import ppddm.core.rest.model.{Agent, Algorithm, AlgorithmParameterName}

case class SupportVectorMachinePPDDM(override val agent: Agent, override val algorithm: Algorithm) extends DataMiningAlgorithm {

  override def getClassifierAndParamGrid(): (Array[PipelineStage], Array[ParamMap]) = {
    // Create the classifier
    val supportVectorClassifier = new LinearSVC()

    // Handle the paramGrid
    val paramGridBuilder = new ParamGridBuilder()
    algorithm.parameters.foreach( p => {
      p.name match {
        case AlgorithmParameterName.MAX_ITER => paramGridBuilder.addGrid(supportVectorClassifier.maxIter, p.getValueAsIntArray())
        case AlgorithmParameterName.REG_PARAM => paramGridBuilder.addGrid(supportVectorClassifier.regParam, p.getValueAsDoubleArray())
        case _ => None
        // Add others here
      }
    })
    val paramGrid = paramGridBuilder.build()

    // Return
    (Array(supportVectorClassifier), paramGrid)
  }
}
