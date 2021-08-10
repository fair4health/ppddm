package ppddm.agent.controller.dm

import akka.Done
import ppddm.agent.controller.dm.DataMiningRequestType.DataMiningRequestType
import ppddm.agent.controller.dm.algorithm.classification.ClassificationAlgorithm
import ppddm.agent.store.AgentDataStoreManager
import ppddm.core.rest.model._
import ppddm.core.util.JsonFormatter._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
 * Controller object for Classification Algorithm Execution
 */
object ClassificationController extends DataMiningController {

  val TRAINING_SIZE = 0.8 // TODO get these values from client in the future
  val TEST_SIZE = 0.2 // TODO get these values from client in the future
  val SEED = 11L // We need a seed value to be the same so that, the data that is split into training and test will always be the same

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

    try {
      // Retrieve the DataFrame object with the given dataset_id previously saved in the data store
      val dataFrame = retrieveDataFrame(modelTrainingRequest.dataset_id)

      logger.debug(s"DataFrame for the Dataset:${modelTrainingRequest.dataset_id} is retrieved for training of ${modelTrainingRequest.algorithms.length} " +
        s"Algorithms in DataMiningModel:${modelTrainingRequest.model_id}")

      // Train and generate weak models on the dataFrame
      val weakModelFutures = modelTrainingRequest.algorithms map { algorithm =>
        ClassificationAlgorithm(modelTrainingRequest.agent, algorithm).train(modelTrainingRequest.dataset_id, dataFrame)
      }

      Future.sequence(weakModelFutures) map { algorithm_models => // Join the Futures
        logger.debug(s"Parallel training of the Algorithms have been completed for DataMiningModel:${modelTrainingRequest.model_id}")

        // Save the ModelTrainingResult containing the models into ppddm-store/models/:model_id
        val modelTrainingResult = ModelTrainingResult(modelTrainingRequest.model_id, modelTrainingRequest.dataset_id,
          modelTrainingRequest.agent, algorithm_models, None)
        saveResult(modelTrainingResult.model_id, DataMiningRequestType.TRAIN, modelTrainingResult.toJson)
      } recover {
        case e: Exception =>
          val msg = s"The following unexpected error occurred while training model:"
          logger.error(msg, e)

          // Save ModelTrainingResult with DataMiningException
          val dataMiningException = DataMiningException(e.getMessage, e)
          val modelTrainingResult = ModelTrainingResult(modelTrainingRequest.model_id, modelTrainingRequest.dataset_id,
            modelTrainingRequest.agent, Seq.empty, Some(dataMiningException.getMessage))
          saveResult(modelTrainingResult.model_id, DataMiningRequestType.TRAIN, modelTrainingResult.toJson)
          throw dataMiningException
      }
    } catch {
      case d: DataMiningException =>
        // Save ModelTrainingResult with DataMiningException
        val modelTrainingResult = ModelTrainingResult(modelTrainingRequest.model_id, modelTrainingRequest.dataset_id,
          modelTrainingRequest.agent, Seq.empty, Some(d.getMessage))
        saveResult(modelTrainingResult.model_id, DataMiningRequestType.TRAIN, modelTrainingResult.toJson)
        throw d
      case e: Exception =>
        val msg = s"An unexpected error occurred while training model"
        logger.error(msg, e)

        // Save ModelTrainingResult with DataMiningException
        val dataMiningException = DataMiningException(msg, e)
        val modelTrainingResult = ModelTrainingResult(modelTrainingRequest.model_id, modelTrainingRequest.dataset_id,
          modelTrainingRequest.agent, Seq.empty, Some(dataMiningException.getMessage))
        saveResult(modelTrainingResult.model_id, DataMiningRequestType.TRAIN, modelTrainingResult.toJson)
        throw dataMiningException
    }
  }

  /**
   * Retrieves the ModelTrainingResult which includes the trained WeakModels and training statistics for the algorithms that were executed
   *
   * @param model_id
   * @return
   */
  def getTrainingResult(model_id: String): Option[ModelTrainingResult] = {
    logger.debug("getTrainingResult received on for model:{}", model_id)
    Try(
      AgentDataStoreManager.getDataFrame(AgentDataStoreManager.getModelPath(model_id, DataMiningRequestType.TRAIN)) map { df =>
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
    logger.debug("deleteTrainingResult received on for model:{}", model_id)
    if (AgentDataStoreManager.deleteDirectory(AgentDataStoreManager.getModelPath(model_id, DataMiningRequestType.TRAIN))) {
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

    try {
      // Retrieve the DataFrame object with the given dataset_id previously saved in the data store
      val dataFrame = retrieveDataFrame(modelValidationRequest.dataset_id)

      logger.debug(s"DataFrame for the Dataset:${modelValidationRequest.dataset_id} is retrieved for validation of ${modelValidationRequest.weak_models.length} " +
        s"WeakModels in DataMiningModel:${modelValidationRequest.model_id}")

      // Split the data into training and test. Only trainingData will be used.
      val Array(trainingData, testData) = dataFrame.randomSplit(Array(TRAINING_SIZE, TEST_SIZE), seed = SEED)

      logger.debug("The DataFrame was split into training/test datasets...")

      // Train each weak model on dataFrame, and calculate statistics for each
      val validationFutures = modelValidationRequest.weak_models.map { weakModel =>
        ClassificationAlgorithm(modelValidationRequest.agent, weakModel.algorithm).validate(weakModel, trainingData)
      }

      // TODO handle errors in futures here

      Future.sequence(validationFutures) map { validationResults => // Join the Futures
        logger.debug(s"Parallel validation of the WeakModels have been completed for DataMiningModel:${modelValidationRequest.model_id}")

        // Save the ModelValidationResult containing the models into ppddm-store/models/:model_id
        val modelValidationResult = ModelValidationResult(modelValidationRequest.model_id, modelValidationRequest.dataset_id,
          modelValidationRequest.agent, validationResults, None)
        saveResult(modelValidationResult.model_id, DataMiningRequestType.VALIDATE, modelValidationResult.toJson)
      } recover {
        case e: Exception =>
          val msg = s"The following unexpected error occurred while validating model:"
          logger.error(msg, e)

          // Save ModelValidationResult with DataMiningException
          val dataMiningException = DataMiningException(e.getMessage, e)
          val modelValidationResult = ModelValidationResult(modelValidationRequest.model_id, modelValidationRequest.dataset_id,
            modelValidationRequest.agent, Seq.empty, Some(dataMiningException.getMessage))
          saveResult(modelValidationResult.model_id, DataMiningRequestType.VALIDATE, modelValidationResult.toJson)
          throw dataMiningException
      }
    } catch {
      case d: DataMiningException =>
        val msg = s"A DataMiningException occurred while validating model"
        logger.error(msg, d)

        // Save ModelValidationResult with DataMiningException
        val modelValidationResult = ModelValidationResult(modelValidationRequest.model_id, modelValidationRequest.dataset_id,
          modelValidationRequest.agent, Seq.empty, Some(d.getMessage))
        saveResult(modelValidationResult.model_id, DataMiningRequestType.VALIDATE, modelValidationResult.toJson)
        throw d
      case e: Exception =>
        val msg = s"An unknown error occurred while validating model"
        logger.error(msg, e)

        // Save ModelValidationResult with Exception
        val dataMiningException = DataMiningException(msg, e)
        val modelValidationResult = ModelValidationResult(modelValidationRequest.model_id, modelValidationRequest.dataset_id,
          modelValidationRequest.agent, Seq.empty, Some(dataMiningException.getMessage))
        saveResult(modelValidationResult.model_id, DataMiningRequestType.VALIDATE, modelValidationResult.toJson)
        throw dataMiningException
    }
  }

  /**
   * Retrieves the ModelValidationResult which includes the validation statistics for the weak models sent by the PPDDM Manager.
   *
   * @param model_id
   * @return
   */
  def getValidationResult(model_id: String): Option[ModelValidationResult] = {
    logger.debug("getValidationResult received on for model:{}", model_id)
    Try(
      AgentDataStoreManager.getDataFrame(AgentDataStoreManager.getModelPath(model_id, DataMiningRequestType.VALIDATE)) map { df =>
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
    logger.debug("deleteValidationResult received on for model:{}", model_id)
    if (AgentDataStoreManager.deleteDirectory(AgentDataStoreManager.getModelPath(model_id, DataMiningRequestType.VALIDATE))) {
      logger.info(s"ModelTrainingResult of model (with id: $model_id) have been deleted successfully")
      Some(Done)
    } else {
      logger.debug(s"ModelTrainingResult of model (with id: $model_id) do not exist!")
      Option.empty[Done]
    }
  }

  /**
   * Start the test of the given BoostedModels within the ModelTestRequest against the Dataset of the Model
   * indicated with model_id.
   *
   * @param modelTestRequest
   * @return
   */
  def startTesting(modelTestRequest: ModelTestRequest): Future[Done] = {
    logger.debug("ModelTestRequest received on agent:{} for model:{} for a total of {} BoostedModels",
      modelTestRequest.agent.agent_id, modelTestRequest.model_id, modelTestRequest.boosted_models.length)

    try {
      // Retrieve the DataFrame object with the given dataset_id previously saved in the data store
      val dataFrame = retrieveDataFrame(modelTestRequest.dataset_id)

      logger.debug(s"DataFrame for the Dataset:${modelTestRequest.dataset_id} is retrieved for testing of ${modelTestRequest.boosted_models.length} " +
        s"BoostedModels in DataMiningModel:${modelTestRequest.model_id}")

      // Split the data into training and test. Only testData will be used.
      val Array(trainingData, testData) = dataFrame.randomSplit(Array(TRAINING_SIZE, TEST_SIZE), seed = SEED)

      // Test each boosted model on dataFrame, and calculate statistics for each
      val testFutures = modelTestRequest.boosted_models.map { boostedModel =>
        ClassificationAlgorithm(modelTestRequest.agent, boostedModel.algorithm).test(boostedModel, testData)
      }

      // TODO handle errors in futures here

      Future.sequence(testFutures) map { testResults => // Join the Futures
        logger.debug(s"Parallel testing of the BoostedModels have been completed for DataMiningModel:${modelTestRequest.model_id}")

        // Save the ModelTestResult containing the models into ppddm-store/models/:model_id
        val modelTestResult = ModelTestResult(modelTestRequest.model_id, modelTestRequest.dataset_id, modelTestRequest.agent, testResults, None)
        saveResult(modelTestResult.model_id, DataMiningRequestType.TEST, modelTestResult.toJson)
      } recover {
        case e: Exception =>
          val msg = s"The following unexpected error occurred while testing model:"
          logger.error(msg, e)

          // Save ModelTestResult with DataMiningException
          val dataMiningException = DataMiningException(msg, e)
          val modelTestResult = ModelTestResult(modelTestRequest.model_id, modelTestRequest.dataset_id,
            modelTestRequest.agent, Seq.empty, Some(dataMiningException.getMessage))
          saveResult(modelTestResult.model_id, DataMiningRequestType.TEST, modelTestResult.toJson)
          throw dataMiningException
      }
    } catch {
      case d: DataMiningException =>
        // Save ModelTestResult with DataMiningException
        val modelTestResult = ModelTestResult(modelTestRequest.model_id, modelTestRequest.dataset_id,
          modelTestRequest.agent, Seq.empty, Some(d.getMessage))
        saveResult(modelTestResult.model_id, DataMiningRequestType.TEST, modelTestResult.toJson)
        throw d
      case e: Exception =>
        val msg = s"An unexpected error occurred while testing model"
        logger.error(msg, e)

        // Save ModelTestResult with Exception
        val dataMiningException = DataMiningException(msg, e)
        val modelTestResult = ModelTestResult(modelTestRequest.model_id, modelTestRequest.dataset_id,
          modelTestRequest.agent, Seq.empty, Some(dataMiningException.getMessage))
        saveResult(modelTestResult.model_id, DataMiningRequestType.TEST, modelTestResult.toJson)
        throw dataMiningException
    }
  }

  /**
   * Retrieves the ModelTestResult which includes the test statistics for the BoostedModels
   *
   * @param model_id
   * @return
   */
  def getTestResult(model_id: String): Option[ModelTestResult] = {
    logger.debug("getTestResult received on for model:{}", model_id)
    Try(
      AgentDataStoreManager.getDataFrame(AgentDataStoreManager.getModelPath(model_id, DataMiningRequestType.TEST)) map { df =>
        df // Dataframe consisting of a column named "value" that holds Json inside
          .head() // Get the Array[Row]
          .getString(0) // Get Json String
          .extract[ModelTestResult]
      }).getOrElse(None) // Returns None if an error occurs within the Try block
  }

  /**
   * Deletes ModelTestResult
   *
   * @param model_id The unique identifier of the model whose ModelTestResult is to be deleted
   * @return
   */
  def deleteTestResult(model_id: String): Option[Done] = {
    logger.debug("deleteTestResult received on for model:{}", model_id)
    if (AgentDataStoreManager.deleteDirectory(AgentDataStoreManager.getModelPath(model_id, DataMiningRequestType.TEST))) {
      logger.info(s"ModelTestResult of model (with id: $model_id) have been deleted successfully")
      Some(Done)
    } else {
      logger.debug(s"ModelTestResult of model (with id: $model_id) do not exist!")
      Option.empty[Done]
    }
  }

  private def saveResult(model_id: String, dataMiningRequestType: DataMiningRequestType, result: String): Done = {
    try {
      // Save the Model Training/Validation/Testing Result containing the models into ppddm-store/models/:model_id
      AgentDataStoreManager.saveDataFrame(
        AgentDataStoreManager.getModelPath(model_id, dataMiningRequestType), Seq(result).toDF())
      logger.debug(s"Model ${dataMiningRequestType} result has been created and persisted into the data store successfully for DataMiningModel:${model_id}")
      Done
    } catch {
      case e: Exception =>
        try {
          logger.debug(s"Exception occured while saving the DataFrame for model:${model_id}. I will try again...")
          // Try saving once more
          AgentDataStoreManager.saveDataFrame(
            AgentDataStoreManager.getModelPath(model_id, dataMiningRequestType), Seq(result).toDF())
          logger.debug(s"Model ${dataMiningRequestType} result has been created and persisted into the data store successfully for DataMiningModel:${model_id}")
          Done
        } catch {
          case e: Exception =>
            val msg = s"Cannot save the Model ${dataMiningRequestType} result of the model with model_id: ${model_id} due to following error:"
            logger.error(msg, e)
            throw DataMiningException(msg, e)
        }
    }
  }

}
