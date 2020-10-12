package ppddm.agent.controller.dm.algorithm

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import ppddm.core.rest.model.{AlgorithmStatisticsName, DataType, Parameter}

import scala.collection.mutable.ListBuffer

object StatisticsUtil {

  def generatePredictionsLabelRDD(dataFrameWithPredictionAndLabel: DataFrame): RDD[(Double, Double)] = {
    dataFrameWithPredictionAndLabel.select("prediction", "label").rdd.map(r => (r.getDouble(0), r.getDouble(1)))
  }

  def generateConfusionMatrixStatistics(predictionLabelsRDD: RDD[(Double, Double)]): ListBuffer[Parameter] = {
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

  def calculateAccuracy(statistics: ListBuffer[Parameter]): Double = {
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
