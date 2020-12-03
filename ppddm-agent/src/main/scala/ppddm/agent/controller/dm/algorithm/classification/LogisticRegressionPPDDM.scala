package ppddm.agent.controller.dm.algorithm.classification

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.ParamGridBuilder
import ppddm.core.rest.model.{Agent, Algorithm, AlgorithmParameterName}

case class LogisticRegressionPPDDM(override val agent: Agent, override val algorithm: Algorithm) extends ClassificationAlgorithm {

  override def getClassifierAndParamGrid(): (Array[PipelineStage], Array[ParamMap]) = {
    // Create the classifier
    val logisticRegression = new LogisticRegression()

    // Handle the paramGrid
    val paramGridBuilder = new ParamGridBuilder()
    algorithm.parameters.foreach( p => {
      p.name match {
        case AlgorithmParameterName.THRESHOLD => paramGridBuilder.addGrid(logisticRegression.threshold, p.getValueAsDoubleArray())
        case AlgorithmParameterName.MAX_ITER => paramGridBuilder.addGrid(logisticRegression.maxIter, p.getValueAsIntArray())
        case AlgorithmParameterName.REG_PARAM => paramGridBuilder.addGrid(logisticRegression.regParam, p.getValueAsDoubleArray())
        case AlgorithmParameterName.ELASTIC_NET_PARAM => paramGridBuilder.addGrid(logisticRegression.elasticNetParam, p.getValueAsDoubleArray())
        case _ => None
        // Add others here
      }
    })
    val paramGrid = paramGridBuilder.build()

    // Return
    (Array(logisticRegression), paramGrid)
  }
}
