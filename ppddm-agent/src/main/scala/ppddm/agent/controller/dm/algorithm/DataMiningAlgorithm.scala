package ppddm.agent.controller.dm.algorithm

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import java.util.Base64

import com.typesafe.scalalogging.Logger
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import org.zeroturnaround.zip.ZipUtil
import ppddm.agent.controller.dm.{BoostedModelManager, StatisticsManager}
import ppddm.agent.exception.DataMiningException
import ppddm.agent.store.DataStoreManager
import ppddm.core.rest.model.{Agent, AgentAlgorithmStatistics, Algorithm, AlgorithmName, BoostedModel, WeakModel}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait DataMiningAlgorithm {

  protected val logger: Logger = Logger(this.getClass)

  protected val agent: Agent // on which aAgent this DataMiningAlgorithm is running now
  protected val algorithm: Algorithm // The Algoritm that this DataMiningAlgorithm is training/validating/testing

  /**
   * Train a model using the DataMiningAlgorithm on the given DataFrame of this Agent
   *
   * @param dataset_id
   * @param dataFrame
   * @return
   */
  def train(dataset_id: String, dataFrame: DataFrame): Future[WeakModel]

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
      val statistics = StatisticsManager.calculateBinaryClassificationStatistics(testPredictionDF)

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

      val testPredictionDF = BoostedModelManager.predictWithWeightedAverageOfPredictions(testPredictionTuples)

      // Calculate statistics
      val statistics = StatisticsManager.calculateBinaryClassificationStatistics(testPredictionDF)

      AgentAlgorithmStatistics(null, agent, algorithm, statistics) // TODO what is first parameter?
    }
  }

  /**
   * Generate a Base64 encoded string of the fitted model of this algorithm
   *
   * @param model A PipelineModel
   * @return
   */
  protected def toString(model: PipelineModel): String = {
    try {
      val path = DataStoreManager.getTmpPath()
      model.save(path)

      val stream = new ByteArrayOutputStream()
      ZipUtil.pack(new File(path), stream)
      val bytes = stream.toByteArray
      stream.close()
      val modelString = Base64.getEncoder.encodeToString(bytes)

      DataStoreManager.deleteDirectory(path)

      modelString
    } catch {
      case e:Exception =>
        val msg = "Error while generating a Base64 encoded string of the trained model."
        logger.error(msg, e)
        throw e
    }
  }

  /**
   * Creates a PipelineModel from the Base64 encoded fitted_model string of this algorithm.
   *
   * @param modelString Base64 encoded string representation of the PipelineModel file content
   * @return A PipelineModel
   */
  protected def fromString(modelString: String): PipelineModel = {
    val path = DataStoreManager.getTmpPath()
    val bytes = Base64.getDecoder.decode(modelString)
    val stream = new ByteArrayInputStream(bytes)
    ZipUtil.unpack(stream, new File(path))
    val pipelineModel = PipelineModel.load(path)
    stream.close()

    DataStoreManager.deleteDirectory(path)

    pipelineModel
  }
}

object DataMiningAlgorithm {
  def apply(agent: Agent, algorithm: Algorithm): DataMiningAlgorithm = {
    algorithm.name match {
      case AlgorithmName.CLASSIFICATION_LOGISTIC_REGRESSION => LogisticRegressionPPDDM(agent, algorithm)
      case AlgorithmName.CLASSIFICATION_DECISION_TREE => DecisionTreePPDDM(agent, algorithm)
      case _ =>
        val msg = s"Unknown Algorithm:${algorithm.name}"
        throw DataMiningException(msg)
    }
  }
}
