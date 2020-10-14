package ppddm.agent.controller.dm

import akka.Done
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import ppddm.agent.Agent
import ppddm.agent.controller.dm.algorithm.DataMiningAlgorithm
import ppddm.agent.exception.DataMiningException
import ppddm.agent.store.DataStoreManager
import ppddm.core.rest.model.{AgentAlgorithmStatistics, ModelTrainingRequest, ModelTrainingResult, ModelValidationRequest, ModelValidationResult}
import ppddm.core.util.JsonFormatter._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

/**
 * Controller object for Data Mining Algorithm Execution
 */
object DataMiningController {

  val TRAINING_SIZE = 0.8 // TODO get these values from client in the future
  val TEST_SIZE = 0.2 // TODO get these values from client in the future
  val SEED = 11L // TODO why do we need this?

  private val logger: Logger = Logger(this.getClass)
  private val sparkSession: SparkSession = Agent.dataMiningEngine.sparkSession

  import sparkSession.implicits._

  /**
   * Start the training of the data mining algorithm. This function returns if the training is started successfully.
   * Model training will continue in the background and the manager will ask about the status through a separate HTTP GET call.
   *
   * @param modelTrainingRequest
   * @return
   */
  def startTraining(modelTrainingRequest: ModelTrainingRequest): Future[Done] = {
    logger.debug("ModelTrainingRequest received on agent:{} for model:{} for a total of {} Algorithms",
      modelTrainingRequest.agent.agent_id, modelTrainingRequest.model_id, modelTrainingRequest.algorithms.length)

    // First prepare the data for execution of the data mining algorithms,
    // i.e. perform the exploratory data analysis which include categorical variable handling, null values handling etc.
    DataAnalysisManager.performDataAnalysis(modelTrainingRequest.dataset_id) flatMap { mlReadyDataFrame =>
      // Then execute the algorithms one by one on the same data to train ML models
      val weakModelFutures = modelTrainingRequest.algorithms map { algorithm =>
        DataMiningAlgorithm(modelTrainingRequest.agent, algorithm).train(mlReadyDataFrame)
      }

      Future.sequence(weakModelFutures) map { algorithm_models => // Join the Futures
        try {
          // After the models are trained (fitted_models are ready within the WeakModels then save the ML-ready DataFrame
          // which was used to train the algorithms of the WeakModels so that it is
          DataStoreManager.saveDataFrame(
            DataStoreManager.getEDAPath(modelTrainingRequest.model_id),
            mlReadyDataFrame)
        } catch {
          case e: Exception =>
            val msg = s"Cannot save the ML-ready DataFrame as a result of the Exploratory Data Analysis for the model with model_id: ${modelTrainingRequest.model_id}."
            logger.error(msg)
            throw DataMiningException(msg, e)
        }

        val modelTrainingResult = ModelTrainingResult(modelTrainingRequest.model_id, modelTrainingRequest.dataset_id,
          modelTrainingRequest.agent, algorithm_models)
        try {
          // Save the ModelTrainingResult containing the models into ppddm-store/models/:model_id
          DataStoreManager.saveDataFrame(
            DataStoreManager.getModelPath(modelTrainingRequest.model_id, DataMiningRequestType.TRAIN),
            Seq(modelTrainingResult.toJson).toDF())
          Done
        } catch {
          case e: Exception =>
            val msg = s"Cannot save the ModelTrainingResult of the model with model_id: ${modelTrainingRequest.model_id}."
            logger.error(msg)
            throw DataMiningException(msg, e)
        }
      }
    }
  }

  /**
   * Retrieves the ModelTrainingResult which includes the trained WeakModels and training statistics for the algorithms that were executed
   *
   * @param model_id
   * @return
   */
  def getTrainingResult(model_id: String): Option[ModelTrainingResult] = {
    Try(
      DataStoreManager.getDataFrame(DataStoreManager.getModelPath(model_id, DataMiningRequestType.TRAIN)) map { df =>
        df // Dataframe consisting of a column named "value" that holds Json inside
          .head() // Get the Array[Row]
          .getString(0) // Get Json String
          .extract[ModelTrainingResult]
      }).getOrElse(None) // Returns None if an error occurs within the Try block
  }

  /**
   * Deletes ModelTrainingResult
   *
   * @param model_id The unique identifier of the model whose ModelTrainingResult is to be deleted
   * @return
   */
  def deleteTrainingResult(model_id: String): Option[Done] = {
    if (DataStoreManager.deleteDirectory(DataStoreManager.getModelPath(model_id, DataMiningRequestType.TRAIN))) {
      logger.info(s"ModelTrainingResult of model (with id: $model_id) have been deleted successfully")
      Some(Done)
    } else {
      logger.debug(s"ModelTrainingResult of model (with id: $model_id) do not exist!")
      Option.empty[Done]
    }
  }

  /**
   * Start the validation of the given WeakModels within the ModelValidationRequest against the Dataset of the Model
   * indicated with model_id.
   *
   * @param modelValidationRequest
   * @return
   */
  def startValidation(modelValidationRequest: ModelValidationRequest): Future[Done] = {
    logger.debug("ModelValidationRequest received on agent:{} for model:{} for a total of {} WeakModels",
      modelValidationRequest.agent.agent_id, modelValidationRequest.model_id, modelValidationRequest.weak_models.length)

    // Retrieve the DataFrame which was saved while generating the fittedModels so that we can transform again and validate
    // the given WeakModel (fitted_models) on this DataFrame
    val dataFrameOption = DataStoreManager.getDataFrame(DataStoreManager.getEDAPath(modelValidationRequest.model_id))
    if (dataFrameOption.isEmpty) {
      val msg = s"ML-ready DataFrame of the model_id with id:${modelValidationRequest.model_id} does not exist in the data store. This should not have happened!!"
      logger.error(msg)
      throw DataMiningException(msg)
    }

    val mlReadyDataFrame = dataFrameOption.get
    // Split the data into training and test
    val Array(trainingData, testData) = mlReadyDataFrame.randomSplit(Array(TRAINING_SIZE, TEST_SIZE), seed = SEED)

    val validationFutures = modelValidationRequest.weak_models.map { weakModel =>
      DataMiningAlgorithm(modelValidationRequest.agent, weakModel.algorithm).validate(weakModel.fitted_model, trainingData)
    }

    Future.sequence(validationFutures) map {validationResults =>
      // FIXME: Fix the following class creation: Do not forget to accumulate the statistics (start with the ones inside the modelValidationRequest
      val modelValidationResult = ModelValidationResult(modelValidationRequest.model_id, modelValidationRequest.dataset_id, modelValidationRequest.agent, Seq.empty[AgentAlgorithmStatistics])

      try {
        // Save the ModelValidationResult containing the models into ppddm-store/models/:model_id
        DataStoreManager.saveDataFrame(
          DataStoreManager.getModelPath(modelValidationResult.model_id, DataMiningRequestType.VALIDATE),
          Seq(modelValidationResult.toJson).toDF())
        Done
      } catch {
        case e: Exception =>
          val msg = s"Cannot save the ModelValidationResult of the model with model_id: ${modelValidationResult.model_id}."
          logger.error(msg)
          throw DataMiningException(msg, e)
      }
    }

  }

  def getValidationResult(model_id: String): Option[ModelValidationResult] = {
    None
  }
}
