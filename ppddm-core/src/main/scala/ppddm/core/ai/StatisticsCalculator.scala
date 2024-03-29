package ppddm.core.ai

import com.typesafe.scalalogging.Logger
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import ppddm.core.rest.model.{AgentAlgorithmStatistics, AlgorithmStatisticsDescription, AlgorithmStatisticsName, DataType, Parameter}

import scala.collection.mutable.ListBuffer

/**
 * This object handles the calculation of statistics
 */
object StatisticsCalculator {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Calculate statistics for binary classification
   * @param testPredictionDF
   * @return
   */
  def calculateBinaryClassificationStatistics(testPredictionDF: DataFrame): ListBuffer[Parameter] = {
    logger.debug("Calculating binary classification statistics...")
    val testPredictionLabelsRDD = StatisticsCalculator.generatePredictionsLabelRDD(testPredictionDF)
    var statistics = StatisticsCalculator.generateConfusionMatrixStatistics(testPredictionLabelsRDD)

    // Calculate statistics
    val metrics = new BinaryClassificationMetrics(testPredictionLabelsRDD)
    statistics += Parameter(AlgorithmStatisticsName.ACCURACY, DataType.DOUBLE, StatisticsCalculator.calculateAccuracy(statistics).toString, Some(AlgorithmStatisticsDescription.ACCURACY))
    statistics += Parameter(AlgorithmStatisticsName.PRECISION, DataType.DOUBLE, metrics.precisionByThreshold().collect().head._2.toString, Some(AlgorithmStatisticsDescription.PRECISION))
    statistics += Parameter(AlgorithmStatisticsName.RECALL, DataType.DOUBLE, metrics.recallByThreshold().collect().head._2.toString, Some(AlgorithmStatisticsDescription.RECALL))
    statistics += Parameter(AlgorithmStatisticsName.F_MEASURE, DataType.DOUBLE, metrics.fMeasureByThreshold().collect().head._2.toString, Some(AlgorithmStatisticsDescription.F_MEASURE))
    statistics += Parameter(AlgorithmStatisticsName.AUROC, DataType.DOUBLE, metrics.areaUnderROC.toString, Some(AlgorithmStatisticsDescription.AUROC))
    statistics += Parameter(AlgorithmStatisticsName.AUPR, DataType.DOUBLE, metrics.areaUnderPR.toString, Some(AlgorithmStatisticsDescription.AUPR))

    logger.debug("Statistics: ")
    statistics.foreach(parameter => logger.debug(s"--- ${parameter.name}: ${parameter.value}"))
    logger.debug("Statistics have been calculated.")

    statistics
  }

  /**
   * Calculate statistics for multinomial classification
   * @param testPredictionDF
   * @return
   */
  def calculateMultinomialClassificationStatistics(testPredictionDF: DataFrame): ListBuffer[Parameter] = {
    logger.debug("Calculating multinomial classification statistics...")
    val testPredictionLabelsRDD = StatisticsCalculator.generatePredictionsLabelRDD(testPredictionDF)
    var statistics = StatisticsCalculator.generateConfusionMatrixStatistics(testPredictionLabelsRDD)

    // Calculate statistics
    val metrics = new MulticlassMetrics(testPredictionLabelsRDD)
    statistics += Parameter(AlgorithmStatisticsName.ACCURACY, DataType.DOUBLE, metrics.accuracy.toString, Some(AlgorithmStatisticsDescription.ACCURACY))
    statistics += Parameter(AlgorithmStatisticsName.PRECISION, DataType.DOUBLE, metrics.weightedPrecision.toString, Some(AlgorithmStatisticsDescription.PRECISION))
    statistics += Parameter(AlgorithmStatisticsName.RECALL, DataType.DOUBLE, metrics.weightedRecall.toString, Some(AlgorithmStatisticsDescription.RECALL))
    statistics += Parameter(AlgorithmStatisticsName.FPR, DataType.DOUBLE, metrics.weightedFalsePositiveRate.toString, Some(AlgorithmStatisticsDescription.FPR))
    statistics += Parameter(AlgorithmStatisticsName.TPR, DataType.DOUBLE, metrics.weightedTruePositiveRate.toString, Some(AlgorithmStatisticsDescription.TPR))
    statistics += Parameter(AlgorithmStatisticsName.F_MEASURE, DataType.DOUBLE, metrics.weightedFMeasure.toString, Some(AlgorithmStatisticsDescription.F_MEASURE))

    logger.debug("Statistics: ")
    statistics.foreach(parameter => logger.debug(s"--- ${parameter.name}: ${parameter.value}"))
    logger.debug("Statistics have been calculated.")

    statistics
  }

  /**
   * Calculate statistics by combining several agents' classification algorithm statistics
   * @param agentAlgorithmStatistics
   * @return
   */
  def combineClassificationStatistics(agentAlgorithmStatistics: Seq[AgentAlgorithmStatistics]): ListBuffer[Parameter] = {
    logger.debug("Combining classification statistics...")

    val total = agentAlgorithmStatistics.map(s => getStatisticsValue(s.statistics, AlgorithmStatisticsName.TOTAL).toInt).sum
    val tp = agentAlgorithmStatistics.map(s => getStatisticsValue(s.statistics, AlgorithmStatisticsName.TRUE_POSITIVE).toInt).sum
    val tn = agentAlgorithmStatistics.map(s => getStatisticsValue(s.statistics, AlgorithmStatisticsName.TRUE_NEGATIVE).toInt).sum
    val fp = agentAlgorithmStatistics.map(s => getStatisticsValue(s.statistics, AlgorithmStatisticsName.FALSE_POSITIVE).toInt).sum
    val fn = agentAlgorithmStatistics.map(s => getStatisticsValue(s.statistics, AlgorithmStatisticsName.FALSE_NEGATIVE).toInt).sum

    var statistics = new ListBuffer[Parameter]
    statistics += Parameter(AlgorithmStatisticsName.TOTAL, DataType.INTEGER, total.toString, Some(AlgorithmStatisticsDescription.TOTAL))
    statistics += Parameter(AlgorithmStatisticsName.TRUE_POSITIVE, DataType.INTEGER, tp.toString, Some(AlgorithmStatisticsDescription.TRUE_POSITIVE))
    statistics += Parameter(AlgorithmStatisticsName.TRUE_NEGATIVE, DataType.INTEGER, tn.toString, Some(AlgorithmStatisticsDescription.TRUE_NEGATIVE))
    statistics += Parameter(AlgorithmStatisticsName.FALSE_POSITIVE, DataType.INTEGER, fp.toString, Some(AlgorithmStatisticsDescription.FALSE_POSITIVE))
    statistics += Parameter(AlgorithmStatisticsName.FALSE_NEGATIVE, DataType.INTEGER, fn.toString, Some(AlgorithmStatisticsDescription.FALSE_NEGATIVE))
    statistics += Parameter(AlgorithmStatisticsName.ACCURACY, DataType.DOUBLE, StatisticsCalculator.calculateAccuracy(statistics).toString, Some(AlgorithmStatisticsDescription.ACCURACY))
    statistics += Parameter(AlgorithmStatisticsName.PRECISION, DataType.DOUBLE, StatisticsCalculator.calculatePrecision(statistics).toString, Some(AlgorithmStatisticsDescription.PRECISION))
    statistics += Parameter(AlgorithmStatisticsName.RECALL, DataType.DOUBLE, StatisticsCalculator.calculateRecall(statistics).toString, Some(AlgorithmStatisticsDescription.RECALL))
    statistics += Parameter(AlgorithmStatisticsName.SPECIFICITY, DataType.DOUBLE, StatisticsCalculator.calculateSpecificity(statistics).toString, Some(AlgorithmStatisticsDescription.SPECIFICITY))
    statistics += Parameter(AlgorithmStatisticsName.F_MEASURE, DataType.DOUBLE, StatisticsCalculator.calculateFMeasure(statistics).toString, Some(AlgorithmStatisticsDescription.F_MEASURE))
    // TODO How to calculate AUROC
    // TODO How to calculate AUPR

    logger.debug("Statistics: ")
    statistics.foreach(parameter => logger.debug(s"--- ${parameter.name}: ${parameter.value}"))
    logger.debug("Statistics have been combined.")

    statistics

  }

  /**
   * For a given list of statistics, find the value of statistics with given name
   * @param statistics
   * @param name
   * @return
   */
  def getStatisticsValue(statistics: Seq[Parameter], name: String): String = {
    statistics.filter(s => s.name == name).head.value
  }

  /**
   * Generate an RDD of "prediction" and "label"
   * @param dataFrameWithPredictionAndLabel
   * @return
   */
  private def generatePredictionsLabelRDD(dataFrameWithPredictionAndLabel: DataFrame): RDD[(Double, Double)] = {
    dataFrameWithPredictionAndLabel.select("prediction", "label").rdd.map(r => (r.getDouble(0), r.getDouble(1)))
  }

  /**
   * Generate confusion matrix
   * @param predictionLabelsRDD
   * @return
   */
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

    val statistics = new ListBuffer[Parameter]
    statistics += Parameter(AlgorithmStatisticsName.TRUE_POSITIVE, DataType.INTEGER, tp.toString, Some(AlgorithmStatisticsDescription.TRUE_POSITIVE))
    statistics += Parameter(AlgorithmStatisticsName.TRUE_NEGATIVE, DataType.INTEGER, tn.toString, Some(AlgorithmStatisticsDescription.TRUE_NEGATIVE))
    statistics += Parameter(AlgorithmStatisticsName.FALSE_POSITIVE, DataType.INTEGER, fp.toString, Some(AlgorithmStatisticsDescription.FALSE_POSITIVE))
    statistics += Parameter(AlgorithmStatisticsName.FALSE_NEGATIVE, DataType.INTEGER, fn.toString, Some(AlgorithmStatisticsDescription.FALSE_NEGATIVE))
    statistics += Parameter(AlgorithmStatisticsName.TOTAL, DataType.INTEGER, (tp + tn + fp + fn).toString, Some(AlgorithmStatisticsDescription.TOTAL))

    statistics
  }

  /**
   * Calculate accuracy
   * @param statistics
   * @return
   */
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

  /**
   * Calculate precision
   * @param statistics
   * @return
   */
  private def calculatePrecision(statistics: ListBuffer[Parameter]): Double = {
    var tp = 0.0
    var fp = 0.0

    statistics.foreach(s => {
      if (s.name == AlgorithmStatisticsName.TRUE_POSITIVE) {
        tp = s.value.toDouble
      } else if (s.name == AlgorithmStatisticsName.FALSE_POSITIVE) {
        fp = s.value.toDouble
      }
    })

    val result = tp / (tp + fp)

    if (result.isNaN) 1.0 else result // If result is NaN, it means that TP + FP is zero. Your data is not good.
  }

  /**
   * Calculate recall
   * @param statistics
   * @return
   */
  private def calculateRecall(statistics: ListBuffer[Parameter]): Double = {
    var tp = 0.0
    var fn = 0.0

    statistics.foreach(s => {
      if (s.name == AlgorithmStatisticsName.TRUE_POSITIVE) {
        tp = s.value.toDouble
      } else if (s.name == AlgorithmStatisticsName.FALSE_NEGATIVE) {
        fn = s.value.toDouble
      }
    })

    val result = tp / (tp + fn)

    if (result.isNaN) 1.0 else result // If result is NaN, it means that TP + FN is zero. Your data is not good.
  }

  /**
   * Calculate specificity
   * @param statistics
   * @return
   */
  private def calculateSpecificity(statistics: ListBuffer[Parameter]): Double = {
    var tn = 0.0
    var fp = 0.0

    statistics.foreach(s => {
      if (s.name == AlgorithmStatisticsName.TRUE_NEGATIVE) {
        tn = s.value.toDouble
      } else if (s.name == AlgorithmStatisticsName.FALSE_POSITIVE) {
        fp = s.value.toDouble
      }
    })

    val result = tn / (tn + fp)

    if (result.isNaN) 1.0 else result // If result is NaN, it means that TN + FP is zero. Your data is not good.
  }

  /**
   * Calculate F-Measure
   * @param statistics
   * @return
   */
  private def calculateFMeasure(statistics: ListBuffer[Parameter]): Double = {
    val precision = calculatePrecision(statistics)
    val recall = calculateRecall(statistics)

    val result = (2 * precision * recall) / (precision + recall)

    if (result.isNaN) 1.0 else result // If result is NaN, it means that precision + recall is zero. Your data is not good.
  }
}
