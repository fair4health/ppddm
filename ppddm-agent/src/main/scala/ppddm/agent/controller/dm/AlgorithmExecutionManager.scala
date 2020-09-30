package ppddm.agent.controller.dm

import com.typesafe.scalalogging.Logger
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.sql.DataFrame
import ppddm.core.rest.model.{Agent, Algorithm, AlgorithmModel, AlgorithmName, AlgorithmParameterName, AlgorithmStatisticsName, DataType, Parameter}

import scala.collection.mutable.ListBuffer
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
   * @param agent The agent
   * @param algorithm Data mining algorithm to be executed
   * @param dataFrame DataFrame object containing "features" and "label"
   * @return
   */
  def executeAlgorithm(agent: Agent, algorithm: Algorithm, dataFrame: DataFrame): Future[AlgorithmModel] = {

    Future {
      algorithm.name match {
        case AlgorithmName.CLASSIFICATION_LOGISTIC_REGRESSION => executeLogisticRegression(agent, algorithm, dataFrame)
      }
    }

  }

  private def executeLogisticRegression(agent: Agent, algorithm: Algorithm, dataFrame: DataFrame): AlgorithmModel = {
    logger.debug("## Start executing logistic regression ##")

    // Create the LogisticRegression object
    val lr = new LogisticRegression()
    algorithm.parameters.foreach( p => {
      val value = p.value.asInstanceOf[String]
      p.name match {
        case AlgorithmParameterName.THRESHOLD => lr.setThreshold(value.toDouble) // TODO check with DataType of Parameter?
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

    var trainingStatistics = new ListBuffer[Parameter]
    var testStatistics = new ListBuffer[Parameter]

    // Calculate statistics
    if (lrModel.numClasses > 2) { // Multinomial Logistic Regression. Output has more than two classes
      // In multinomial, it does not make sense to see statistics by label. Instead, use weighted statistics

      logger.debug("Calculating training statistics for Multinomial Logistic Regression...")
      val trainingSummary = lrModel.summary
      trainingStatistics += Parameter(AlgorithmStatisticsName.ACCURACY, DataType.DOUBLE, trainingSummary.accuracy)
      trainingStatistics += Parameter(AlgorithmStatisticsName.PRECISION, DataType.DOUBLE, trainingSummary.weightedPrecision)
      trainingStatistics += Parameter(AlgorithmStatisticsName.RECALL, DataType.DOUBLE, trainingSummary.weightedRecall)
      trainingStatistics += Parameter(AlgorithmStatisticsName.FPR, DataType.DOUBLE, trainingSummary.weightedFalsePositiveRate)
      trainingStatistics += Parameter(AlgorithmStatisticsName.TPR, DataType.DOUBLE, trainingSummary.weightedTruePositiveRate)
      trainingStatistics += Parameter(AlgorithmStatisticsName.F_MEASURE, DataType.DOUBLE, trainingSummary.weightedFMeasure)
      trainingStatistics.foreach(parameter => logger.debug(s"${parameter.name}: ${parameter.value}\n"))
      logger.debug("Training statistics have been calculated for Multinomial Logistic Regression")

      logger.debug("Calculating test statistics for Multinomial Logistic Regression...")
      val metrics = new MulticlassMetrics(predictionLabelsRDD)
      testStatistics += Parameter(AlgorithmStatisticsName.ACCURACY, DataType.DOUBLE, metrics.accuracy)
      testStatistics += Parameter(AlgorithmStatisticsName.PRECISION, DataType.DOUBLE, metrics.weightedPrecision)
      testStatistics += Parameter(AlgorithmStatisticsName.RECALL, DataType.DOUBLE, metrics.weightedRecall)
      testStatistics += Parameter(AlgorithmStatisticsName.FPR, DataType.DOUBLE, metrics.weightedFalsePositiveRate)
      testStatistics += Parameter(AlgorithmStatisticsName.TPR, DataType.DOUBLE, metrics.weightedTruePositiveRate)
      testStatistics += Parameter(AlgorithmStatisticsName.F_MEASURE, DataType.DOUBLE, metrics.weightedFMeasure)
      testStatistics.foreach(parameter => logger.debug(s"${parameter.name}: ${parameter.value}\n"))
      logger.debug("Test statistics have been calculated for Multinomial Logistic Regression")

    } else { // Binomial Logistic Regression. Output has two classes, i.e. 1 and 0
      // In binomial, it does make sense to see statistics by label. Hence, use label statistics

      logger.debug("Calculating training statistics for Binomial Logistic Regression...")
      val trainingSummary = lrModel.binarySummary
      trainingStatistics += Parameter(AlgorithmStatisticsName.ACCURACY, DataType.DOUBLE, trainingSummary.accuracy)
      trainingStatistics += Parameter(AlgorithmStatisticsName.PRECISION, DataType.DOUBLE, trainingSummary.weightedPrecision)
      trainingStatistics += Parameter(AlgorithmStatisticsName.RECALL, DataType.DOUBLE, trainingSummary.weightedRecall)
      trainingStatistics += Parameter(AlgorithmStatisticsName.FPR, DataType.DOUBLE, trainingSummary.weightedFalsePositiveRate)
      trainingStatistics += Parameter(AlgorithmStatisticsName.TPR, DataType.DOUBLE, trainingSummary.weightedTruePositiveRate)
      trainingStatistics += Parameter(AlgorithmStatisticsName.F_MEASURE, DataType.DOUBLE, trainingSummary.weightedFMeasure)
      trainingStatistics.foreach(parameter => logger.debug(s"${parameter.name}: ${parameter.value}\n"))
      logger.debug("Training statistics have been calculated for Binomial Logistic Regression")

      logger.debug("Calculating test statistics for Binomial Logistic Regression...")
      val metrics = new BinaryClassificationMetrics(predictionLabelsRDD) // TODO update here
      testStatistics += Parameter(AlgorithmStatisticsName.PRECISION, DataType.DOUBLE,  metrics.precisionByThreshold().collect().head._2)
      testStatistics += Parameter(AlgorithmStatisticsName.RECALL, DataType.DOUBLE, metrics.recallByThreshold().collect().head._2)
      testStatistics += Parameter(AlgorithmStatisticsName.F_MEASURE, DataType.DOUBLE, metrics.fMeasureByThreshold().collect().head._2)
      testStatistics += Parameter(AlgorithmStatisticsName.AUROC, DataType.DOUBLE, metrics.areaUnderROC)
      testStatistics += Parameter(AlgorithmStatisticsName.AUPR, DataType.DOUBLE, metrics.areaUnderPR)
      testStatistics.foreach(parameter => logger.debug(s"${parameter.name}: ${parameter.value}\n"))
      logger.debug("Test statistics have been calculated for Binomial Logistic Regression")
    }

    logger.debug("## Finish executing logistic regression ##")
    AlgorithmModel(algorithm, agent, trainingStatistics, testStatistics, null) // TODO return the model

  }

}
