package ppddm.manager.controller.dm

import com.mongodb.client.model.ReturnDocument
import com.typesafe.scalalogging.Logger
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.FindOneAndReplaceOptions
import ppddm.core.exception.DBException
import ppddm.core.rest.model.{Agent, DataMiningModel, SelectionStatus, WeakModel}
import ppddm.manager.Manager
import ppddm.manager.exception.DataIntegrityException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Controller object for the DataMiningModels.
 *
 * A DataMiningModel is created within a Project by providing a Dataset and sequence of Algorithms to be executed
 * on the Dataset. Since the Dataset is distributed over the Agents (DatasetSources), execution of the algorithms
 * is performed with the help of DistributedDataMiningManager. Distributed algorithm execution should be orchestrated
 * for each Algorithm and the final model should be always updated in the associated DataMiningModel on Platform
 * Repository.
 */
object DataMiningModelController {

  val COLLECTION_NAME: String = "dataminingmodel"

  private val logger: Logger = Logger(this.getClass)
  private val db = Manager.mongoDB.getDatabase

  /**
   * Returns a list of the SelectionStatus.SELECTED Agents of the Dataset of the given dataMiningModel.
   *
   * Before returning the list of Agents, this function performs a number of integrity checks and throws
   * DataIntegrityException accordingly.
   *
   * @param dataMiningModel
   * @return
   */
  def getSelectedAgents(dataMiningModel: DataMiningModel): Seq[Agent] = {
    if (dataMiningModel.dataset.dataset_sources.isEmpty || dataMiningModel.dataset.dataset_sources.get.isEmpty) {
      // This is a data integrity problem. This should not happen!
      val msg = s"You want to get the selected Agents for this DataMiningModel with model_id:${dataMiningModel.model_id} and " +
        s"name:${dataMiningModel.name} HOWEVER there are no DatasetSources in the Dataset of this DataMiningModel. " +
        s"dataset_id:${dataMiningModel.dataset.dataset_id.get} and dataset_name:${dataMiningModel.dataset.name}"
      logger.error(msg)
      throw DataIntegrityException(msg)
    }

    if (dataMiningModel.dataset.dataset_sources.get.exists(_.selection_status.isEmpty)) {
      val msg = s"There is at least one DatasetSource within the Dataset of this DataMiningModel whose selection_status is None! " +
        s"model_id:${dataMiningModel.model_id} model_name:${dataMiningModel.name} dataset_id:${dataMiningModel.dataset.dataset_id}"
      logger.error(msg)
      throw DataIntegrityException(msg)
    }

    // Find the Agents to be connected for data mining (those are the SELECTED ones for the Dataset)
    dataMiningModel.dataset.dataset_sources.get
      .filter(_.selection_status.get == SelectionStatus.SELECTED)
      .map(_.agent)
  }

  /**
   * Given the dataMiningModel, returns the sequence of Agents whose training results have not been received yet.
   *
   * @param dataMiningModel
   * @return
   */
  def getAgentsWaitedForTrainingResults(dataMiningModel: DataMiningModel): Seq[Agent] = {
    val agentsWhoseTrainingResultsAlreadyReceieved =
      if (dataMiningModel.boosted_models.isDefined) {
        dataMiningModel.boosted_models.get
          .head // Use the first BoostedModel since we are sure! that all BoostedModels have results from the same Agents at any time
          .weak_models.map(_.agent) // Get the Agent of each existing WeakModel
          .toSet // Convert to Set
      } else {
        Set.empty[Agent]
      }
    (getSelectedAgents(dataMiningModel).toSet -- agentsWhoseTrainingResultsAlreadyReceieved).toSeq
  }

  /**
   * Given the dataMiningModel, returns a sequence of tuples in the form of (_1, _2) where:
   * _1 is the Agent
   * _2 is the sequence of WeakModels to be validated on the Agent (_1). These are the WeakModels which were trained on
   * the other Agents.
   *
   * When a WeakModel is trained on an Agent, it should be validated on the *other* Agents.
   *
   * @param dataMiningModel
   * @return
   */
  def getAgentValidationModelPairs(dataMiningModel: DataMiningModel): Seq[(Agent, Seq[WeakModel])] = {
    if (dataMiningModel.boosted_models.isEmpty) {
      val msg = s"Hey boy, there are no BoostedModels for this DataMiningModel:${dataMiningModel.model_id.get} and you want me to " +
        s"extract the Agent-ValidationWeakModels pairs. I cannot do it."
      logger.error(msg)
      throw DataIntegrityException(msg)
    }

    // Get the Agents from whom WeakModels should already have been received.
    getSelectedAgents(dataMiningModel).map { agent => // For each Agent
      val weakModelsToBeValidatedOnAgent = dataMiningModel.boosted_models.get.flatMap { boostedModel => // Loop through the BoostedModels of this DataMiningModel
        boostedModel.weak_models.filterNot(_.agent.agent_id == agent.agent_id) // Find the WeakModels within each BoostedModel whose Agent is not the agent we are looping over
      }
      agent -> weakModelsToBeValidatedOnAgent
    }
  }

  /**
   * Given the dataMiningModel, returns the sequence of Agents whose validation results have not been received yet.
   *
   * @param dataMiningModel
   * @return
   */
  def getAgentsWaitedForValidationResults(dataMiningModel: DataMiningModel): Seq[Agent] = {
    checkBoostedModelIntegrity(dataMiningModel)

    // If there were no data inconsistency, all BoostedModels of this dataMiningModel should have the WeakModels from the
    // very same Agents at any instant in time.
    val legalAgents = dataMiningModel.boosted_models.get
      .map(_.weak_models.map(_.agent).toSet) // Create a set from the Agents of each WeakModel
      .reduce((a, b) => if (a.equals(b)) a else Set.empty)
    if (legalAgents.isEmpty) {
      val msg = s"Ooops! All the WeakModels of the BoostedModels within a DataMiningModel:${dataMiningModel.model_id.get} should include SAME " +
        s"Agents at any instant in time. It seems this is not the case!!!"
      logger.error(msg)
      throw DataIntegrityException(msg)
    }

    val agentsWhoseValidationResultsAlreadyReceieved = dataMiningModel.boosted_models.get.head // Use the first BoostedModel since all will have the results from the same Agents at any instant in time
      .weak_models.flatMap { weakModel => // for each WeakModel of this BoostedModel
      weakModel.validation_statistics
        .filter(s => s.agent_model.agent_id != s.agent_statistics.agent_id)
        .map(_.agent_statistics) // Collect the Agents from whom statistics are received
        .toSet // Convert to a Set
    }

    (getSelectedAgents(dataMiningModel).toSet -- agentsWhoseValidationResultsAlreadyReceieved).toSeq
  }

  private def checkBoostedModelIntegrity(dataMiningModel: DataMiningModel): Unit = {
    if (dataMiningModel.boosted_models.isEmpty) {
      val msg = s"Hey boy, there are no BoostedModels for this DataMiningModel:${dataMiningModel.model_id.get} and you want me to " +
        s"find the Agents whose validation/test results are being waited. I cannot do it."
      logger.error(msg)
      throw DataIntegrityException(msg)
    }

    // And if we are calling this function, then we are sure that a BoostedModel is there for each Algorithm
    val boostedModelsAlgorithms = dataMiningModel.boosted_models.get.map(_.algorithm).toSet
    if (!boostedModelsAlgorithms.equals(dataMiningModel.algorithms.toSet)) {
      val msg = s"There must be one BoostedModel for each Algorithm of this DataMiningModel:${dataMiningModel.model_id.get}"
      logger.error(msg)
      throw DataIntegrityException(msg)
    }
  }

  /**
   * Given the dataMiningModel, returns the sequence of Agents whose test results have not been received yet.
   *
   * @param dataMiningModel
   * @return
   */
  def getAgentsWaitedForTestResults(dataMiningModel: DataMiningModel): Seq[Agent] = {
    checkBoostedModelIntegrity(dataMiningModel)

    val agentsWhoseTestResultsAlreadyReceieved = dataMiningModel.boosted_models.get.head // Use the first BoostedModel since all BoostedModels will contain results from the very same Agents at any instant in time.
      .test_statistics.getOrElse(Seq.empty)
      .map(_.agent_statistics) // Collect the Agents from whom statistics are received

    (getSelectedAgents(dataMiningModel).toSet -- agentsWhoseTestResultsAlreadyReceieved).toSeq
  }

  /**
   * Creates a new DataMiningModel on the Platform Repository
   * and starts the distributed data mining orchestration for the created DataMiningModel.
   *
   * @param dataMiningModel The DataMiningModel to be created
   * @return The created DataMiningModel with a unique model_id in it
   */
  def createDataMiningModel(dataMiningModel: DataMiningModel): Future[DataMiningModel] = {
    // Create a new DataMiningModel object with a unique identifier
    val dataMiningModelWithId = dataMiningModel.withUniqueModelId

    db.getCollection[DataMiningModel](COLLECTION_NAME).insertOne(dataMiningModelWithId).toFuture() // insert into the database
      .map { result =>
        val _id = result.getInsertedId.asObjectId().getValue.toString
        logger.debug("Inserted document _id:{} and model_id:{}", _id, dataMiningModelWithId.model_id.get)
        dataMiningModelWithId
      }
      .recover {
        case e: Exception =>
          val msg = s"Error while inserting a DataMiningModel with model_id:${dataMiningModelWithId.model_id.get} into the database."
          throw DBException(msg, e)
      }
  }

  /**
   * Retrieves the DataMiningModel from the Platform Repository.
   *
   * @param model_id The unique identifier of the DataMiningModel
   * @return The DataMiningModel if model_id is valid, None otherwise.
   */
  def getDataMiningModel(model_id: String): Future[Option[DataMiningModel]] = {
    db.getCollection[DataMiningModel](COLLECTION_NAME).find(equal("model_id", model_id))
      .first()
      .headOption()
      .recover {
        case e: Exception =>
          val msg = s"Error while retrieving the DataMiningModel with model_id:${model_id} from the database."
          throw DBException(msg, e)
      }
  }

  /**
   * Retrieves all DataMiningModels of a Project from the Platform Repository.
   *
   * @param project_id The project ID whose Datasets are to be retrieved.
   * @return The list of all DataMiningModels for the given project, empty list if there are no DataMiningModels.
   */
  def getAllDataMiningModels(project_id: String): Future[Seq[DataMiningModel]] = {
    db.getCollection[DataMiningModel](COLLECTION_NAME).find(equal("project_id", project_id)).toFuture()
      .recover {
        case e: Exception =>
          val msg = s"Error while retrieving the DataMiningModels of the Project with project_id:${project_id} from the database."
          throw DBException(msg, e)
      }
  }

  /**
   * Updates the DataMiningModel by doing a replacement.
   *
   * @param dataMiningModel The DataMiningModel object to be updated.
   * @return The updated DataMiningModel object if operation is successful, None otherwise.
   */
  def updateDataMiningModel(dataMiningModel: DataMiningModel): Future[Option[DataMiningModel]] = {
    db.getCollection[DataMiningModel](COLLECTION_NAME).findOneAndReplace(
      equal("model_id", dataMiningModel.model_id.get),
      dataMiningModel.withUpdatedDataMiningState(),
      FindOneAndReplaceOptions().returnDocument(ReturnDocument.AFTER))
      .headOption()
      .recover {
        case e: Exception =>
          val msg = s"Error while updating the DataMiningModel with model_id:${dataMiningModel.model_id.get}."
          throw DBException(msg, e)
      }
  }

  /**
   * Deletes DataMiningModel from the Platform Repository.
   *
   * @param model_id The unique identifier of the DataMiningModel to be deleted.
   * @return The deleted Dataset object if operation is successful, None otherwise.
   */
  def deleteDataMiningModel(model_id: String): Future[Option[DataMiningModel]] = {
    db.getCollection[DataMiningModel](COLLECTION_NAME).findOneAndDelete(equal("model_id", model_id))
      .headOption()
      .recover {
        case e: Exception =>
          val msg = s"Error while deleting the DataMiningModel with model_id:${model_id}."
          throw DBException(msg, e)
      }
  }

}
