package ppddm.agent.controller.dm.algorithm

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.DataFrame
import ppddm.agent.controller.dm.{DataAnalysisManager, StatisticsManager}
import ppddm.agent.controller.dm.DataMiningController.{SEED, TEST_SIZE, TRAINING_SIZE}
import ppddm.core.rest.model.{Agent, AgentAlgorithmStatistics, Algorithm, AlgorithmParameterName, WeakModel}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

private case class DecisionTreePPDDM(override val agent: Agent, override val algorithm: Algorithm) extends DataMiningAlgorithm {

  override def train(dataset_id: String, dataFrame: DataFrame): Future[WeakModel] = {
    logger.debug("## Start executing DecisionTree ##")

    // Prepare the data for execution of the data mining algorithms,
    // i.e. perform the exploratory data analysis which include categorical variable handling, null values handling etc.
    DataAnalysisManager.performDataAnalysis(dataset_id, dataFrame) map { pipelineStages =>

      // TODO we can also perform Train-Validation Split here if we parameters as array from the client.

      // Create the LogisticRegression object
      val decisionTree = new DecisionTreeClassifier()

      // Split the data into training and test
      val Array(trainingData, testData) = dataFrame.randomSplit(Array(TRAINING_SIZE, TEST_SIZE), seed = SEED)

      val pipeline = new Pipeline().setStages(pipelineStages ++ Array(decisionTree))

      // ### Apply k-fold cross validation ###
      logger.debug("Applying k-fold cross-validation...")

      var numberOfFolds = 3 // Use 3+ in practice
      var maxParallelism = 2 // Evaluate up to 2 parameter settings in parallel
      val paramGridBuilder = new ParamGridBuilder()
      algorithm.parameters.foreach( p => {
        p.name match {
          case AlgorithmParameterName.MAX_DEPTH => paramGridBuilder.addGrid(decisionTree.maxDepth, p.getValueAsIntArray())
          case AlgorithmParameterName.MIN_INFO_GAIN => paramGridBuilder.addGrid(decisionTree.minInfoGain, p.getValueAsDoubleArray())
          case AlgorithmParameterName.IMPURITY => paramGridBuilder.addGrid(decisionTree.impurity, p.getValueAsStringArray())
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
      logger.debug("Fitting DecisionTreeClassificationModel...")
      val cvModel = cv.fit(trainingData)
      val pipelineModel = cvModel.bestModel.asInstanceOf[PipelineModel]
      logger.debug("DecisionTreeClassificationModel has been fit.")

      // Test the model
      logger.debug("Testing the model with test data...")
      val testPredictionDF = pipelineModel.transform(testData)
      logger.debug("DecisionTreeClassificationModel has been tested.")

      // Calculate statistics
      val statistics = StatisticsManager.calculateBinaryClassificationStatistics(testPredictionDF)

      logger.debug("## Finish executing logistic regression ##")

      WeakModel(algorithm, agent, toString(pipelineModel), Seq(AgentAlgorithmStatistics(agent, agent, algorithm, statistics)), None, None, None)
    }
  }
}
