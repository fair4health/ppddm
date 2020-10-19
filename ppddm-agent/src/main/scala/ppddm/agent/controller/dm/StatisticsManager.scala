package ppddm.agent.controller.dm

import com.typesafe.scalalogging.Logger
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import ppddm.core.rest.model.{AlgorithmStatisticsName, DataType, Parameter}

import scala.collection.mutable.ListBuffer

object StatisticsManager {

  private val logger: Logger = Logger(this.getClass)

  def calculateBinaryClassificationStatistics(testPredictionDF: DataFrame): ListBuffer[Parameter] = {
    logger.debug("Calculating statistics...")
    val testPredictionLabelsRDD = StatisticsManager.generatePredictionsLabelRDD(testPredictionDF)
    var statistics = StatisticsManager.generateConfusionMatrixStatistics(testPredictionLabelsRDD)

    // Calculate statistics
    val metrics = new BinaryClassificationMetrics(testPredictionLabelsRDD)
    statistics += Parameter(AlgorithmStatisticsName.ACCURACY, DataType.DOUBLE, StatisticsManager.calculateAccuracy(statistics))
    statistics += Parameter(AlgorithmStatisticsName.PRECISION, DataType.DOUBLE, metrics.precisionByThreshold().collect().head._2)
    statistics += Parameter(AlgorithmStatisticsName.RECALL, DataType.DOUBLE, metrics.recallByThreshold().collect().head._2)
    statistics += Parameter(AlgorithmStatisticsName.F_MEASURE, DataType.DOUBLE, metrics.fMeasureByThreshold().collect().head._2)
    statistics += Parameter(AlgorithmStatisticsName.AUROC, DataType.DOUBLE, metrics.areaUnderROC)
    statistics += Parameter(AlgorithmStatisticsName.AUPR, DataType.DOUBLE, metrics.areaUnderPR)

    logger.debug("Statistics: ")
    statistics.foreach(parameter => logger.debug(s"--- ${parameter.name}: ${parameter.value}"))
    logger.debug("Statistics have been calculated.")

    statistics
  }

  def calculateMultinomialClassificationStatistics(testPredictionDF: DataFrame): ListBuffer[Parameter] = {
    logger.debug("Calculating statistics...")
    val testPredictionLabelsRDD = StatisticsManager.generatePredictionsLabelRDD(testPredictionDF)
    var statistics = StatisticsManager.generateConfusionMatrixStatistics(testPredictionLabelsRDD)

    // Calculate statistics
    val metrics = new MulticlassMetrics(testPredictionLabelsRDD)
    statistics += Parameter(AlgorithmStatisticsName.ACCURACY, DataType.DOUBLE, metrics.accuracy)
    statistics += Parameter(AlgorithmStatisticsName.PRECISION, DataType.DOUBLE, metrics.weightedPrecision)
    statistics += Parameter(AlgorithmStatisticsName.RECALL, DataType.DOUBLE, metrics.weightedRecall)
    statistics += Parameter(AlgorithmStatisticsName.FPR, DataType.DOUBLE, metrics.weightedFalsePositiveRate)
    statistics += Parameter(AlgorithmStatisticsName.TPR, DataType.DOUBLE, metrics.weightedTruePositiveRate)
    statistics += Parameter(AlgorithmStatisticsName.F_MEASURE, DataType.DOUBLE, metrics.weightedFMeasure)

    logger.debug("Statistics: ")
    statistics.foreach(parameter => logger.debug(s"--- ${parameter.name}: ${parameter.value}"))
    logger.debug("Statistics have been calculated.")

    statistics
  }

  private def generatePredictionsLabelRDD(dataFrameWithPredictionAndLabel: DataFrame): RDD[(Double, Double)] = {
    dataFrameWithPredictionAndLabel.select("prediction", "label").rdd.map(r => (r.getDouble(0), r.getDouble(1)))
  }

  private def generateConfusionMatrixStatistics(predictionLabelsRDD: RDD[(Double, Double)]): ListBuffer[Parameter] = {
    var tp = 0
    var tn = 0
    var fp = 0
    var fn = 0

    predictionLabelsRDD.collect().map(r => {
      val prediction = r._1
      val label = r._2

      if (prediction == 1 && label == 1) {
        tp += 1
      } else if (prediction == 0 && label == 0) {
        tn += 1
      } else if (prediction == 1 && label == 0) {
        fp += 1
      } else if (prediction == 0 && label == 1) {
        fn += 1
      }
    })

    var statistics = new ListBuffer[Parameter]
    statistics += Parameter(AlgorithmStatisticsName.TRUE_POSITIVE, DataType.INTEGER, tp)
    statistics += Parameter(AlgorithmStatisticsName.TRUE_NEGATIVE, DataType.INTEGER, tn)
    statistics += Parameter(AlgorithmStatisticsName.FALSE_POSITIVE, DataType.INTEGER, fp)
    statistics += Parameter(AlgorithmStatisticsName.FALSE_NEGATIVE, DataType.INTEGER, fn)
    statistics += Parameter(AlgorithmStatisticsName.TOTAL, DataType.INTEGER, tp + tn + fp + fn)

    statistics
  }

  private def calculateAccuracy(statistics: ListBuffer[Parameter]): Double = {
    var tp = 0.0
    var tn = 0.0
    var total = 0.0

    statistics.foreach(s => {
      if (s.name == AlgorithmStatisticsName.TRUE_POSITIVE) {
        tp = s.value.toDouble
      } else if (s.name == AlgorithmStatisticsName.TRUE_NEGATIVE) {
        tn = s.value.toDouble
      } else if (s.name == AlgorithmStatisticsName.TOTAL) {
        total = s.value.toDouble
      }
    })

    (tp + tn) / total
  }

}
