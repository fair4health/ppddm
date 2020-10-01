package ppddm.manager.controller.dm

import com.mongodb.client.model.ReturnDocument
import com.typesafe.scalalogging.Logger
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.FindOneAndReplaceOptions
import ppddm.core.exception.DBException
import ppddm.core.rest.model.{DataMiningModel, DataMiningSource}
import ppddm.manager.Manager

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
   * Creates a new DataMiningModel on the Platform Repository
   * and invokes the agents to start their algorithm execution processes.
   *
   * @param dataMiningModel The DataMiningModel to be created
   * @return The created DataMiningModel with a unique model_id in it
   */
  def createDataMiningModel(dataMiningModel: DataMiningModel): Future[DataMiningModel] = {
    // Create a new DataMiningModel object with a unique identifier
    val dataMiningModelWithId = dataMiningModel.withUniqueModelId

    // Invoke agents to start data mining (algorithm execution) processes
    // Returns a new Dataset object with data sources and execution state "querying"
    DistributedDataMiningManager.invokeAgentsDataMining(dataMiningModelWithId) flatMap { dataMiningModelWithDataMiningSources =>
      logger.info("Algorithm execution (data mining) endpoints are invoked for the selected Agents (datasources) of the associated Dataset")
      db.getCollection[DataMiningModel](COLLECTION_NAME).insertOne(dataMiningModelWithDataMiningSources).toFuture() // insert into the database
        .map { result =>
          val _id = result.getInsertedId.asObjectId().getValue.toString
          logger.debug("Inserted document _id:{} and model_id:{}", _id, dataMiningModelWithDataMiningSources.model_id.get)
          dataMiningModelWithDataMiningSources
        }
        .recover {
          case e: Exception =>
            val msg = s"Error while inserting a DataMiningModel with model_id:${dataMiningModelWithDataMiningSources.model_id.get} into the database."
            throw DBException(msg, e)
        }
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
      .flatMap { dataMiningModelOption =>
        if (dataMiningModelOption.isDefined) { // If the dataMiningModelOption is found with the given model_id
          // Ask the algorithm execution results to its DataMiningSources
          DistributedDataMiningManager.askAgentsDataMiningResults(dataMiningModelOption.get) map (Some(_))
        } else {
          Future {
            None
          }
        }
      }
  }

  /**
   * Retrieves all DataMiningModels of a Project from the Platform Repository.
   *
   * @param project_id The project ID whose Datasets are to be retrieved.
   * @return The list of all DataMiningModels for the given project, empty list if there are no DataMiningModels.
   */
  def getAllDataMiningModels(project_id: String): Future[Seq[DataMiningModel]] = {
    db.getCollection[DataMiningModel](COLLECTION_NAME).find(equal("project_id", project_id)).toFuture() flatMap { dataMiningModels =>
      Future.sequence(
        // Ask the data preparation results for each dataset to their DatasetSources
        // Do this job in parallel and then join with Future.sequence to return a Future[Seq[Dataset]]
        dataMiningModels.map(dataMiningModel => DistributedDataMiningManager.askAgentsDataMiningResults(dataMiningModel))
      )
    }
  }

  /**
   * Updates the DataMiningModel by doing a replacement.
   *
   * @param dataMiningModel The DataMiningModel object to be updated.
   * @return The updated DataMiningModel object if operation is successful, None otherwise.
   */
  def updateDataMiningModel(dataMiningModel: DataMiningModel): Future[Option[DataMiningModel]] = {
    // TODO: Add some integrity checks before document replacement
    // TODO: Check whether at least one Algorithm is selected
    db.getCollection[DataMiningModel](COLLECTION_NAME).findOneAndReplace(
      equal("model_id", dataMiningModel.model_id.get),
      dataMiningModel.withUpdatedExecutionState(),
      FindOneAndReplaceOptions().returnDocument(ReturnDocument.AFTER)).headOption()
  }

  /**
   * Deletes DataMiningModel from the Platform Repository.
   *
   * @param model_id The unique identifier of the DataMiningModel to be deleted.
   * @return The deleted Dataset object if operation is successful, None otherwise.
   */
  def deleteDataMiningModel(model_id: String): Future[Option[DataMiningModel]] = {
    db.getCollection[DataMiningModel](COLLECTION_NAME).findOneAndDelete(equal("model_id", model_id)).headOption() flatMap { dataMiningModelOption: Option[DataMiningModel] =>
      if (dataMiningModelOption.isDefined) {
        val dataMiningModel = dataMiningModelOption.get
        Future.sequence(
          dataMiningModel.data_mining_sources.get.map { dataMiningSource: DataMiningSource => // For each DataMiningSource in this set
            DistributedDataMiningManager.deleteAlgorithmExecutionResult(dataMiningSource.agent, dataMiningModel) // Delete the algorithm execution results from the Agents (do this in parallel)
          }) map { _ => Some(dataMiningModel) }
      }
      else {
        Future.apply(Option.empty[DataMiningModel])
      }
    }
  }

}
