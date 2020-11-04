package ppddm.agent.controller.dm.algorithm

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{GBTClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.DataFrame
import ppddm.agent.controller.dm.DataAnalysisManager
import ppddm.agent.controller.dm.DataMiningController.{SEED, TEST_SIZE, TRAINING_SIZE}
import ppddm.core.ai.StatisticsCalculator
import ppddm.core.rest.model.{Agent, AgentAlgorithmStatistics, Algorithm, AlgorithmParameterName, WeakModel}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

private case class GradientBoostedTreePPDDM(override val agent: Agent, override val algorithm: Algorithm) extends DataMiningAlgorithm {

  override def train(dataset_id: String, dataFrame: DataFrame): Future[WeakModel] = {
    logger.debug("## Start executing GradientBoostedTree ##")

    // Prepare the data for execution of the data mining algorithms,
    // i.e. perform the exploratory data analysis which include categorical variable handling, null values handling etc.
    DataAnalysisManager.performDataAnalysis(dataset_id, dataFrame) map { pipelineStages =>

      // TODO we can also perform Train-Validation Split here if we parameters as array from the client.

      // Create the GBTClassifier object
      val gbtClassifier = new GBTClassifier()

      // Split the data into training and test. Only trainingData will be used.
      val Array(trainingData, testData) = dataFrame.randomSplit(Array(TRAINING_SIZE, TEST_SIZE), seed = SEED)

      val pipeline = new Pipeline().setStages(pipelineStages ++ Array(gbtClassifier))

      // ### Apply k-fold cross validation ###
      logger.debug("Applying k-fold cross-validation...")

      var numberOfFolds = 3 // Use 3+ in practice
      var maxParallelism = 2 // Evaluate up to 2 parameter settings in parallel
      val paramGridBuilder = new ParamGridBuilder()
      algorithm.parameters.foreach( p => {
        p.name match {
          case AlgorithmParameterName.MAX_DEPTH => paramGridBuilder.addGrid(gbtClassifier.maxDepth, p.getValueAsIntArray())
          case AlgorithmParameterName.MIN_INFO_GAIN => paramGridBuilder.addGrid(gbtClassifier.minInfoGain, p.getValueAsDoubleArray())
          case AlgorithmParameterName.FEATURE_SUBSET_STRATEGY => paramGridBuilder.addGrid(gbtClassifier.featureSubsetStrategy, p.getValueAsStringArray())
          case AlgorithmParameterName.MAX_ITER => paramGridBuilder.addGrid(gbtClassifier.maxIter, p.getValueAsIntArray())
          case AlgorithmParameterName.NUMBER_OF_FOLDS => numberOfFolds = p.value.toInt
          case AlgorithmParameterName.MAX_PARALLELISM => maxParallelism = p.value.toInt
          // Add others here
        }
      })
      val paramGrid = paramGridBuilder.build()
      val binaryClassificationEvaluator = new BinaryClassificationEvaluator()
        .setLabelCol("label")
        .setRawPredictionCol("rawPrediction")
        .setMetricName("areaUnderROC") // TODO Decide which metric to use. It can be precision/recall for imbalanced data, and accuracy for others
      val cv = new CrossValidator()
        .setEstimator(pipeline)
        .setEvaluator(binaryClassificationEvaluator)
        .setEstimatorParamMaps(paramGrid)
        .setNumFolds(numberOfFolds)
        .setParallelism(maxParallelism)

      // Fit the model
      logger.debug("Fitting GradientBoostedTreeClassifier...")
      val cvModel = cv.fit(trainingData)
      val pipelineModel = cvModel.bestModel.asInstanceOf[PipelineModel]
      logger.debug("GradientBoostedTreeClassifier has been fit.")

      // Test the model
      logger.debug("Testing the model with test data...")
      val testPredictionDF = pipelineModel.transform(trainingData)
      logger.debug("GradientBoostedTreeClassificationModel has been tested.")

      // Calculate statistics
      val statistics = StatisticsCalculator.calculateBinaryClassificationStatistics(testPredictionDF)

      logger.debug("## Finish executing GradientBoostedTree ##")

      WeakModel(algorithm, agent, toString(pipelineModel), AgentAlgorithmStatistics(agent, agent, algorithm, statistics), Seq.empty, None, None)
    }
  }
}
