package ppddm.agent.controller.dm.algorithm.classification

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.DataFrame
import ppddm.agent.controller.dm.ClassificationController.{SEED, TEST_SIZE, TRAINING_SIZE}
import ppddm.agent.controller.dm.DataAnalysisManager
import ppddm.agent.controller.dm.algorithm.DataMiningAlgorithm
import ppddm.core.ai.{Predictor, StatisticsCalculator}
import ppddm.core.rest.model.{Agent, AgentAlgorithmStatistics, Algorithm, AlgorithmName, AlgorithmParameterName, BoostedModel, DataMiningException, WeakModel}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait ClassificationAlgorithm extends DataMiningAlgorithm {

  /**
   * Get classifier and paramGrid for cross validation for each classification algorithm
   *
   * @return tuple of classifier and paramGrid
   */
  def getClassifierAndParamGrid(): (Array[PipelineStage], Array[ParamMap])

  /**
   * Train a model using the ClassificationAlgorithm on the given DataFrame of this Agent
   *
   * @param dataset_id
   * @param dataFrame
   * @return
   */
  def train(dataset_id: String, dataFrame: DataFrame): Future[WeakModel] = {
    logger.debug(s"## Start executing ${algorithm.name} ##")

    // Prepare the data for execution of the data mining algorithms,
    // i.e. perform the exploratory data analysis which include categorical variable handling, null values handling etc.
    DataAnalysisManager.performDataAnalysis(dataset_id, dataFrame) map { pipelineStages =>

      // TODO we can also perform Train-Validation Split here if we parameters as array from the client.

      // Create the classifier object and paramGrid containing its parameters for cross validation
      val classifierAndParamGrid = getClassifierAndParamGrid()

      // Split the data into training and test. Only trainingData will be used.
      val Array(trainingData, testData) = dataFrame.randomSplit(Array(TRAINING_SIZE, TEST_SIZE), seed = SEED)

      val pipeline = new Pipeline().setStages(pipelineStages ++ classifierAndParamGrid._1)

      // ### Apply k-fold cross validation ###
      logger.debug("Applying k-fold cross-validation...")

      var numberOfFolds = 3 // Use 3+ in practice
      var maxParallelism = 2 // Evaluate up to 2 parameter settings in parallel
      var metric = "areaUnderROC" // TODO Decide which metric to use. It can be precision/recall for imbalanced data, and accuracy for others
      algorithm.parameters.foreach(p => {
        p.name match {
          case AlgorithmParameterName.NUMBER_OF_FOLDS => numberOfFolds = p.value.toInt
          case AlgorithmParameterName.MAX_PARALLELISM => maxParallelism = p.value.toInt
          case AlgorithmParameterName.METRIC => metric = p.value
          case _ => None
        }
      })
      val binaryClassificationEvaluator = new BinaryClassificationEvaluator()
        .setLabelCol("label")
        .setRawPredictionCol("rawPrediction")
        .setMetricName(metric)
      val cv = new CrossValidator()
        .setEstimator(pipeline)
        .setEvaluator(binaryClassificationEvaluator)
        .setEstimatorParamMaps(classifierAndParamGrid._2)
        .setNumFolds(numberOfFolds)
        .setParallelism(maxParallelism)

      // Fit the model
      logger.debug(s"Fitting ${algorithm.name} model...")
      val cvModel = cv.fit(trainingData)
      val pipelineModel = cvModel.bestModel.asInstanceOf[PipelineModel]
      logger.debug(s"${algorithm.name} model has been fit.")

      // Test the model
      logger.debug("Testing the model with test data...")
      val testPredictionDF = pipelineModel.transform(trainingData)
      logger.debug(s"${algorithm.name} model has been tested.")

      // Calculate statistics
      val statistics = StatisticsCalculator.calculateBinaryClassificationStatistics(testPredictionDF)

      logger.debug(s"## Finish executing ${algorithm.name} ##")

      WeakModel(algorithm = algorithm,
        agent = agent,
        fitted_model = Some(toString(pipelineModel)),
        training_statistics = Some(AgentAlgorithmStatistics(agent, agent, algorithm, statistics)),
        validation_statistics = Some(Seq.empty[AgentAlgorithmStatistics]),
        calculated_statistics = None,
        weight = None,
        item_frequencies = None,
        total_record_count = None,
        frequent_itemsets = None,
        association_rules = None
      )
    }
  }

  /**
   * Validate a model on the given dataFrame
   *
   * @param weakModel The WeakModel on which validation is to be performed
   * @param dataFrame The DataFrame which will be used for validation on the .transform method
   * @return
   */
  def validate(weakModel: WeakModel, dataFrame: DataFrame): Future[AgentAlgorithmStatistics] = {
    Future {
      val pipelineModel = fromString(weakModel.fitted_model.get)
      val testPredictionDF = pipelineModel.transform(dataFrame)

      // Calculate statistics
      val statistics = StatisticsCalculator.calculateBinaryClassificationStatistics(testPredictionDF)

      AgentAlgorithmStatistics(weakModel.agent, agent, algorithm, statistics)
    }
  }

  /**
   * Validate a boosted model on the given dataFrame
   *
   * @param boostedModel The BoostedModel on which test is to be performed
   * @param dataFrame    The DataFrame which will be used for test on the .transform method
   * @return
   */
  def test(boostedModel: BoostedModel, dataFrame: DataFrame): Future[AgentAlgorithmStatistics] = {
    Future {
      val testPredictionTuples = boostedModel.weak_models.map { weakModel =>
        val pipelineModel = fromString(weakModel.fitted_model.get)
        (weakModel.weight.get, pipelineModel.transform(dataFrame))
      }

      val testPredictionDF = Predictor.predictWithWeightedAverageOfPredictions(testPredictionTuples)

      // Calculate statistics
      val statistics = StatisticsCalculator.calculateBinaryClassificationStatistics(testPredictionDF)

      AgentAlgorithmStatistics(agent, agent, algorithm, statistics)
    }
  }
}

object ClassificationAlgorithm {
  def apply(agent: Agent, algorithm: Algorithm): ClassificationAlgorithm = {
    algorithm.name match {
      case AlgorithmName.CLASSIFICATION_LOGISTIC_REGRESSION => LogisticRegressionPPDDM(agent, algorithm)
      case AlgorithmName.CLASSIFICATION_DECISION_TREE => DecisionTreePPDDM(agent, algorithm)
      case AlgorithmName.CLASSIFICATION_RANDOM_FOREST => RandomForestPPDDM(agent, algorithm)
      case AlgorithmName.CLASSIFICATION_GBT => GradientBoostedTreePPDDM(agent, algorithm)
      case AlgorithmName.CLASSIFICATION_SVM => SupportVectorMachinePPDDM(agent, algorithm)
      case AlgorithmName.CLASSIFICATION_NAIVE_BAYES => NaiveBayesPPDDM(agent, algorithm)
      case _ =>
        val msg = s"Unknown Algorithm:${algorithm.name}"
        throw DataMiningException(msg)
    }
  }
}
