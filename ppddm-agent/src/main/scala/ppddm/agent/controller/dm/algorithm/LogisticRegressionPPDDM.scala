package ppddm.agent.controller.dm.algorithm

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.sql.DataFrame
import ppddm.core.rest.model._
import ppddm.agent.controller.dm.DataMiningController.{SEED, TEST_SIZE, TRAINING_SIZE}

import scala.concurrent.Future
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global

private case class LogisticRegressionPPDDM(override val agent: Agent, override val algorithm: Algorithm) extends DataMiningAlgorithm {

  override def train(dataFrame: DataFrame): Future[WeakModel] = {
    Future {
      logger.debug("## Start executing LogisticRegression ##")

      // TODO we can also perform Train-Validation Split here if we parameters as array from the client.

      // Create the LogisticRegression object
      val logisticRegression = new LogisticRegression()

      // Split the data into training and test
      val Array(trainingData, testData) = dataFrame.randomSplit(Array(TRAINING_SIZE, TEST_SIZE), seed = SEED)

      val pipeline = new Pipeline().setStages(Array(logisticRegression))

      // ### Apply k-fold cross validation ###
      logger.debug("Applying k-fold cross-validation...")

      var numberOfFolds = 3 // Use 3+ in practice
      var maxParallelism = 2 // Evaluate up to 2 parameter settings in parallel
      val paramGridBuilder = new ParamGridBuilder()
      algorithm.parameters.foreach( p => {
        p.name match {
          case AlgorithmParameterName.THRESHOLD => paramGridBuilder.addGrid(logisticRegression.threshold, p.getValueAsDoubleArray())
          case AlgorithmParameterName.MAX_ITER => paramGridBuilder.addGrid(logisticRegression.maxIter, p.getValueAsIntArray())
          case AlgorithmParameterName.REG_PARAM => paramGridBuilder.addGrid(logisticRegression.regParam, p.getValueAsDoubleArray())
          case AlgorithmParameterName.ELASTIC_NET_PARAM => paramGridBuilder.addGrid(logisticRegression.elasticNetParam, p.getValueAsDoubleArray())
          case AlgorithmParameterName.NUMBER_OF_FOLDS => numberOfFolds = p.value.toInt
          case AlgorithmParameterName.MAX_PARALLELISM => maxParallelism = p.value.toInt
          // Add others here
        }
      })
      val paramGrid = paramGridBuilder.build()

      val cv = new CrossValidator()
        .setEstimator(pipeline)
        .setEvaluator(new BinaryClassificationEvaluator)
        .setEstimatorParamMaps(paramGrid)
        .setNumFolds(numberOfFolds)
        .setParallelism(maxParallelism)


      // Fit the model
      logger.debug("Fitting LogisticRegressionModel...")
      val cvModel = cv.fit(trainingData)
      val pipelineModel = cvModel.bestModel.asInstanceOf[PipelineModel]
      logger.debug("LogisticRegressionModel has been fit.")

      // Test the model
      logger.debug("Testing the model with test data...")
      val predictionDF = pipelineModel.transform(testData)
      val predictionLabelsRDD = predictionDF.select("prediction", "label").rdd.map(r => (r.getDouble(0), r.getDouble(1)))
      logger.debug("LogisticRegressionModel has been tested.")

      var trainingStatistics = new ListBuffer[Parameter]
      var testStatistics = new ListBuffer[Parameter]

      // We are sure that there is only one stage since we add only LinearRegression above
      val logisticRegressionModel = pipelineModel.stages.head.asInstanceOf[LogisticRegressionModel]

      // Calculate statistics
      if (logisticRegressionModel.numClasses > 2) { // Multinomial Logistic Regression. Output has more than two classes
        // In multinomial, it does not make sense to see statistics by label. Instead, use weighted statistics

        logger.debug("Calculating training statistics for Multinomial Logistic Regression...")
        val trainingSummary = logisticRegressionModel.summary
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
        val trainingSummary = logisticRegressionModel.binarySummary
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
        testStatistics += Parameter(AlgorithmStatisticsName.PRECISION, DataType.DOUBLE, metrics.precisionByThreshold().collect().head._2)
        testStatistics += Parameter(AlgorithmStatisticsName.RECALL, DataType.DOUBLE, metrics.recallByThreshold().collect().head._2)
        testStatistics += Parameter(AlgorithmStatisticsName.F_MEASURE, DataType.DOUBLE, metrics.fMeasureByThreshold().collect().head._2)
        testStatistics += Parameter(AlgorithmStatisticsName.AUROC, DataType.DOUBLE, metrics.areaUnderROC)
        testStatistics += Parameter(AlgorithmStatisticsName.AUPR, DataType.DOUBLE, metrics.areaUnderPR)
        testStatistics.foreach(parameter => logger.debug(s"${parameter.name}: ${parameter.value}\n"))
        logger.debug("Test statistics have been calculated for Binomial Logistic Regression")
      }

      logger.debug("## Finish executing logistic regression ##")

      WeakModel(algorithm, agent, toString(pipelineModel), Seq(AgentAlgorithmStatistics(agent, agent, algorithm, trainingStatistics)), None, None, None)
    }
  }

}
