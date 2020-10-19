package ppddm.agent.controller.dm

import akka.Done
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import ppddm.agent.Agent
import ppddm.agent.controller.dm.algorithm.DataMiningAlgorithm
import ppddm.agent.exception.DataMiningException
import ppddm.agent.store.DataStoreManager
import ppddm.core.rest.model.{ModelTrainingRequest, ModelTrainingResult, ModelValidationRequest, ModelValidationResult}
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
  val SEED = 11L // We need a seed value to be the same so that, the data that is split into training and test will always be the same

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

    // Retrieve the DataFrame object with the given dataset_id previously saved in the data store
    val dataFrame = retrieveDataFrame(modelTrainingRequest.dataset_id)

    // Train and generate weak models on the dataFrame
    val weakModelFutures = modelTrainingRequest.algorithms map { algorithm =>
      DataMiningAlgorithm(modelTrainingRequest.agent, algorithm).train(modelTrainingRequest.dataset_id, dataFrame)
    }

    Future.sequence(weakModelFutures) map { algorithm_models => // Join the Futures

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

    // Retrieve the DataFrame object with the given dataset_id previously saved in the data store
    val dataFrame = retrieveDataFrame(modelValidationRequest.dataset_id)

    // Split the data into training and test. Only trainingData will be used.
    val Array(trainingData, testData) = dataFrame.randomSplit(Array(TRAINING_SIZE, TEST_SIZE), seed = SEED)

    // Train each weak model on dataFrame, and calculate statistics for each
    val validationFutures = modelValidationRequest.weak_models.map { weakModel =>
      DataMiningAlgorithm(modelValidationRequest.agent, weakModel.algorithm).validate(weakModel, trainingData)
    }

    Future.sequence(validationFutures) map { validationResults => // Join the Futures
      val modelValidationResult = ModelValidationResult(modelValidationRequest.model_id, modelValidationRequest.dataset_id, modelValidationRequest.agent, validationResults)

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

  /**
   * Retrieves the ModelValidationResult which includes the validation statistics for the weak models sent by the PPDDM Manager.
   *
   * @param model_id
   * @return
   */
  def getValidationResult(model_id: String): Option[ModelValidationResult] = {
    Try(
      DataStoreManager.getDataFrame(DataStoreManager.getModelPath(model_id, DataMiningRequestType.VALIDATE)) map { df =>
        df // Dataframe consisting of a column named "value" that holds Json inside
          .head() // Get the Array[Row]
          .getString(0) // Get Json String
          .extract[ModelValidationResult]
      }).getOrElse(None) // Returns None if an error occurs within the Try block
  }

  /**
   * Deletes ModelTrainingResult
   *
   * @param model_id The unique identifier of the model whose ModelTrainingResult is to be deleted
   * @return
   */
  def deleteValidationResult(model_id: String): Option[Done] = {
    if (DataStoreManager.deleteDirectory(DataStoreManager.getModelPath(model_id, DataMiningRequestType.VALIDATE))) {
      logger.info(s"ModelTrainingResult of model (with id: $model_id) have been deleted successfully")
      Some(Done)
    } else {
      logger.debug(s"ModelTrainingResult of model (with id: $model_id) do not exist!")
      Option.empty[Done]
    }
  }

  /**
   * Retrieves already saved DataFrame from DataStore
   * @param dataset_id the id of dataset to be retrieved
   * @return the DataFrame if it exists. If not, throws a DataMiningException
   */
  private def retrieveDataFrame(dataset_id: String): DataFrame = {
    val dataFrameOption = DataStoreManager.getDataFrame(DataStoreManager.getDatasetPath(dataset_id))
    if (dataFrameOption.isEmpty) {
      val msg = s"The Dataset with id:${dataset_id} on which Data Mining algorithms will be executed does not exist. This should not have happened!!"
      logger.error(msg)
      throw DataMiningException(msg)
    }
    dataFrameOption.get
  }
}
