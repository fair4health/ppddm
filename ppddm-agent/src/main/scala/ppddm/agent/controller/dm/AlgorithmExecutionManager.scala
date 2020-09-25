package ppddm.agent.controller.dm

import akka.Done
import com.typesafe.scalalogging.Logger
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.sql.DataFrame
import ppddm.core.rest.model.{Algorithm, AlgorithmName, AlgorithmParameterName, Parameter}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * This object handles the execution of Data Mining algorithms on the given DataFrame
 */
object AlgorithmExecutionManager {

  private val logger: Logger = Logger(this.getClass)

  private val TRAINING_SIZE = 0.8 // TODO get these values from client in the future
  private val TEST_SIZE = 0.2 // TODO get these values from client in the future
  private val SEED = 11L // TODO why do we need this?

  /**
   * Execute the algorithm on the given DataFrame containing "features" and "label"
   * @param algorithm Data mining algorithm to be executed
   * @param dataFrame DataFrame object containing "features" and "label"
   * @return
   */
  def executeAlgorithm(algorithm: Algorithm, dataFrame: DataFrame): Future[Done] = {

    algorithm.name match {
      case AlgorithmName.CLASSIFICATION_LOGISTIC_REGRESSION => executeLogisticRegression(algorithm.parameters, dataFrame)
    }

    Future.apply(Done)
  }

  private def executeLogisticRegression(parameters: Seq[Parameter], dataFrame: DataFrame): MLWritable = {
    logger.debug("## Start executing logistic regression ##")

    // Create the LogisticRegression object
    val lr = new LogisticRegression()
    parameters.foreach( p => {
      val value = p.value.asInstanceOf[String]
      p.name match {
        case AlgorithmParameterName.THRESHOLD => lr.setThreshold(value.toDouble)
        case AlgorithmParameterName.MAX_ITER => lr.setMaxIter(value.toInt)
        case AlgorithmParameterName.REG_PARAM => lr.setRegParam(value.toDouble)
        case AlgorithmParameterName.ELASTIC_NET_PARAM => lr.setElasticNetParam(value.toDouble)
        // Add others here
      }
    })

    // TODO we can also perform Cross-Validation and Train-Validation Split here if we parameters as array from the client.
    // TODO However, in such a case, we need to update the return type.

    // Split the data into training and test
    val Array(trainingData, testData) = dataFrame.randomSplit(Array(TRAINING_SIZE, TEST_SIZE), seed = SEED)

    // Fit the model
    logger.debug("Fitting LogisticRegressionModel...")
    val lrModel = lr.fit(trainingData)
    logger.debug("LogisticRegressionModel has been fit.")

    // Test the model
    logger.debug("Testing the model with test data...")
    val predictionDF = lrModel.transform(testData)
    val predictionLabelsRDD = predictionDF.select("prediction", "label").rdd.map(r => (r.getDouble(0), r.getDouble(1)))
    logger.debug("LogisticRegressionModel has been tested.")

    // Calculate statistics
    if (lrModel.numClasses > 2) { // Multinomial Logistic Regression. Output has more than two classes
      // In multinomial, it does not make sense to see statistics by label. Instead, use weighted statistics

      logger.debug("Calculating training statistics for Multinomial Logistic Regression...")
      val trainingSummary = lrModel.summary
      val trainingAccuracy = trainingSummary.accuracy
      val trainingPrecision = trainingSummary.weightedPrecision
      val trainingRecall = trainingSummary.weightedRecall
      val trainingFalsePositiveRate = trainingSummary.weightedFalsePositiveRate
      val trainingTruePositiveRate = trainingSummary.weightedTruePositiveRate
      val trainingFMeasure = trainingSummary.weightedFMeasure
      logger.debug(s"Accuracy: $trainingAccuracy\nPrecision: $trainingPrecision\nRecall: $trainingRecall\n" +
          s"FPR: $trainingFalsePositiveRate\nTPR: $trainingTruePositiveRate\n" +
          s"F-measure: $trainingFMeasure")
      logger.debug("Training statistics have been calculated for Multinomial Logistic Regression")

      logger.debug("Calculating test statistics for Multinomial Logistic Regression...")
      val metrics = new MulticlassMetrics(predictionLabelsRDD)
      val testAccuracy = metrics.accuracy
      val testPrecision = metrics.weightedPrecision
      val testRecall = metrics.weightedRecall
      val testFalsePositiveRate = metrics.weightedFalsePositiveRate
      val testTruePositiveRate = metrics.weightedTruePositiveRate
      val testFMeasure = metrics.weightedFMeasure
      logger.debug(s"Accuracy: $testAccuracy\nPrecision: $testPrecision\nRecall: $testRecall\n" +
        s"FPR: $testFalsePositiveRate\nTPR: $testTruePositiveRate\n" +
        s"F-measure: $testFMeasure")
      logger.debug("Test statistics have been calculated for Multinomial Logistic Regression")

    } else { // Binomial Logistic Regression. Output has two classes, i.e. 1 and 0
      // In binomial, it does make sense to see statistics by label. Hence, use label statistics

      logger.debug("Calculating training statistics for Binomial Logistic Regression...")
      val trainingSummary = lrModel.binarySummary
      val trainingAccuracy = trainingSummary.accuracy
      val trainingPrecision = trainingSummary.weightedPrecision
      val trainingRecall = trainingSummary.weightedRecall
      val trainingFalsePositiveRate = trainingSummary.weightedFalsePositiveRate
      val trainingTruePositiveRate = trainingSummary.weightedTruePositiveRate
      val trainingFMeasure = trainingSummary.weightedFMeasure
      logger.debug(s"Accuracy: $trainingAccuracy\nPrecision: $trainingPrecision\nRecall: $trainingRecall\n" +
        s"FPR: $trainingFalsePositiveRate\nTPR: $trainingTruePositiveRate\n" +
        s"F-measure: $trainingFMeasure")
      logger.debug("Training statistics have been calculated for Binomial Logistic Regression")

      logger.debug("Calculating test statistics for Binomial Logistic Regression...")
      val metrics = new BinaryClassificationMetrics(predictionLabelsRDD) // TODO update here
      val testPrecision = metrics.precisionByThreshold().collect().head._2
      val testRecall = metrics.recallByThreshold().collect().head._2
      val testFMeasure = metrics.fMeasureByThreshold().collect().head._2
      val testAreaUnderPR = metrics.areaUnderPR
      val testareaUnderROC = metrics.areaUnderROC
      logger.debug(s"Precision: $testPrecision\nRecall: $testRecall\nF-measure: $testFMeasure\n" +
        s"AreaUnderPR: $testAreaUnderPR\nAreaUnderROC: $testareaUnderROC\n")
      logger.debug("Test statistics have been calculated for Binomial Logistic Regression")

    }

    logger.debug("## Finish executing logistic regression ##")
    lrModel

  }

}
