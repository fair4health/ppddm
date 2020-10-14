package ppddm.manager.controller.dm

import com.mongodb.client.model.ReturnDocument
import com.typesafe.scalalogging.Logger
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.FindOneAndReplaceOptions
import ppddm.core.exception.DBException
import ppddm.core.rest.model.{Agent, DataMiningModel, SelectionStatus}
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

    // FIXME: Check whether all dataset_sources have a selection_status or not before executing the following statements
    if(dataMiningModel.dataset.dataset_sources.get.exists(_.selection_status.isEmpty)) {
      val msg = ""
      logger.error(msg)
      throw DataIntegrityException(msg)
    }

    // Find the Agents to be connected for data mining (those are the SELECTED ones for the Dataset)
    dataMiningModel.dataset.dataset_sources.get
      .filter(_.selection_status.get == SelectionStatus.SELECTED)
      .map(_.agent)
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
  }

  /**
   * Retrieves all DataMiningModels of a Project from the Platform Repository.
   *
   * @param project_id The project ID whose Datasets are to be retrieved.
   * @return The list of all DataMiningModels for the given project, empty list if there are no DataMiningModels.
   */
  def getAllDataMiningModels(project_id: String): Future[Seq[DataMiningModel]] = {
    db.getCollection[DataMiningModel](COLLECTION_NAME).find(equal("project_id", project_id)).toFuture()
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
      dataMiningModel,
      FindOneAndReplaceOptions().returnDocument(ReturnDocument.AFTER)).headOption()
  }

  /**
   * Deletes DataMiningModel from the Platform Repository.
   *
   * @param model_id The unique identifier of the DataMiningModel to be deleted.
   * @return The deleted Dataset object if operation is successful, None otherwise.
   */
  def deleteDataMiningModel(model_id: String): Future[Option[DataMiningModel]] = {
    db.getCollection[DataMiningModel](COLLECTION_NAME).findOneAndDelete(equal("model_id", model_id)).headOption()
//    flatMap { dataMiningModelOption: Option[DataMiningModel] =>
//      if (dataMiningModelOption.isDefined) {
//        val dataMiningModel = dataMiningModelOption.get
//        Future.sequence(
//          dataMiningModel.data_mining_sources.get.map { dataMiningSource: DataMiningSource => // For each DataMiningSource in this set
//            DistributedDataMiningManager.deleteAlgorithmExecutionResult(dataMiningSource.agent, dataMiningModel) // Delete the algorithm execution results from the Agents (do this in parallel)
//          }) map { _ => Some(dataMiningModel) }
//      }
//      else {
//        Future.apply(Option.empty[DataMiningModel])
//      }
//    }
  }

}
