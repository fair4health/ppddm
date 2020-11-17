package ppddm.agent.controller.dm.algorithm

import com.typesafe.scalalogging.Logger
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.sql.{DataFrame, SparkSession}
import ppddm.agent.controller.dm.DataAnalysisManager
import ppddm.agent.controller.dm.DataMiningController.{SEED, TEST_SIZE, TRAINING_SIZE}
import ppddm.agent.exception.DataMiningException
import ppddm.agent.store.AgentDataStoreManager
import ppddm.core.ai.{PipelineModelEncoderDecoder, Predictor, StatisticsCalculator}
import ppddm.core.rest.model.{Agent, AgentAlgorithmStatistics, Algorithm, AlgorithmName, AlgorithmParameterName, BoostedModel, WeakModel}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait DataMiningAlgorithm {

  implicit val sparkSession: SparkSession = ppddm.agent.Agent.dataMiningEngine.sparkSession
  protected val logger: Logger = Logger(this.getClass)

  protected val agent: Agent // on which aAgent this DataMiningAlgorithm is running now
  protected val algorithm: Algorithm // The Algoritm that this DataMiningAlgorithm is training/validating/testing

  /**
   * Get classifier and paramGrid for cross validation for each classification algorithm
   * @return tuple of classifier and paramGrid
   */
  def getClassifierAndParamGrid(): (Array[PipelineStage], Array[ParamMap])

  /**
   * Train a model using the DataMiningAlgorithm on the given DataFrame of this Agent
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
      algorithm.parameters.foreach( p => {
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

      WeakModel(algorithm, agent, toString(pipelineModel), AgentAlgorithmStatistics(agent, agent, algorithm, statistics), Seq.empty, None, None)
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
      val pipelineModel = fromString(weakModel.fitted_model)
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
   * @param dataFrame The DataFrame which will be used for test on the .transform method
   * @return
   */
  def test(boostedModel: BoostedModel, dataFrame: DataFrame): Future[AgentAlgorithmStatistics] = {
    Future {
      val testPredictionTuples = boostedModel.weak_models.map { weakModel =>
        val pipelineModel = fromString(weakModel.fitted_model)
        (weakModel.weight.get, pipelineModel.transform(dataFrame))
      }

      val testPredictionDF = Predictor.predictWithWeightedAverageOfPredictions(testPredictionTuples)

      // Calculate statistics
      val statistics = StatisticsCalculator.calculateBinaryClassificationStatistics(testPredictionDF)

      AgentAlgorithmStatistics(agent, agent, algorithm, statistics)
    }
  }

  /**
   * Generate a Base64 encoded string of the fitted model of this algorithm
   *
   * @param model A PipelineModel
   * @return
   */
  def toString(model: PipelineModel): String = {
    PipelineModelEncoderDecoder.toString(model, AgentDataStoreManager.getTmpPath())
  }

  /**
   * Creates a PipelineModel from the Base64 encoded fitted_model string of this algorithm.
   *
   * @param modelString Base64 encoded string representation of the PipelineModel file content
   * @return A PipelineModel
   */
  def fromString(modelString: String): PipelineModel = {
    PipelineModelEncoderDecoder.fromString(modelString, AgentDataStoreManager.getTmpPath())
  }

}

object DataMiningAlgorithm {
  def apply(agent: Agent, algorithm: Algorithm): DataMiningAlgorithm = {
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
