package ppddm.manager.controller.dataset

import com.typesafe.scalalogging.Logger
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.{FindOneAndReplaceOptions, ReturnDocument}
import ppddm.core.exception.DBException
import ppddm.core.rest.model.{Dataset, ExecutionState}
import ppddm.manager.Manager
import ppddm.manager.controller.query.FederatedQueryManager

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Controller object for the Datasets.
 * A Dataset mainly contains a Featureset, an eligibility query and a set of DataSources such that:
 *  - The eligibility query is executed on each Datasource (on the FHIR Repositories)
 *  - The results are extracted/converted according to the given Featureset. The values of each variable within the Featureset is extracted.
 *  - This query execution and data execution is performed on each Datasource asynchronously.
 */
object DatasetController {

  val COLLECTION_NAME: String = "dataset"

  private val logger: Logger = Logger(this.getClass)
  private val db = Manager.mongoDB.getDatabase

  /**
   * Creates a new Dataset on the Platform Repository
   * and invokes the agents to start their data extraction/preparation processes.
   *
   * @param dataset The Dataset to be created
   * @return The created Dataset with a unique dataset_id in it
   */
  def createDataset(dataset: Dataset): Future[Dataset] = {
    // Create a new Dataset object with a unique identifier
    val datasetWithId = dataset.withUniqueDatasetId

    // Invoke agents to start data preparation processes
    // Returns a new Dataset object with data sources and execution state "querying"
    FederatedQueryManager.invokeAgentsDataPreparation(datasetWithId) flatMap { datasetWithDataSources =>
      logger.info("Data preparation endpoints are invoked for all registered Agents (datasources)")
      db.getCollection[Dataset](COLLECTION_NAME).insertOne(datasetWithDataSources).toFuture() // insert into the database
        .map { result =>
          val _id = result.getInsertedId.asObjectId().getValue.toString
          logger.debug("Inserted document _id:{} and datasetId:{}", _id, datasetWithDataSources.dataset_id.get)
          datasetWithDataSources
        }
        .recover {
          case e: Exception =>
            val msg = s"Error while inserting a Dataset with dataset_id:${datasetWithDataSources.dataset_id.get} into the database."
            logger.error(msg, datasetWithDataSources.dataset_id.get, e)
            throw DBException(msg, e)
        }
    }
  }

  /**
   * Retrieves the Dataset from the Platform Repository.
   *
   * @param dataset_id The unique identifier of the Dataset
   * @return The Dataset if dataset_id is valid, None otherwise.
   */
  def getDataset(dataset_id: String): Future[Option[Dataset]] = {
    db.getCollection[Dataset](COLLECTION_NAME).find(equal("dataset_id", dataset_id))
      .first()
      .headOption()
      .flatMap { datasetOption =>
        if (datasetOption.isDefined) { // If the dataset is found with the given dataset_id
          // Ask the data preparation results to its DatasetSources
          FederatedQueryManager.askAgentsDataPreparationResults(datasetOption.get) map (Some(_))
        } else {
          Future {
            None
          }
        }
      }
  }

  /**
   * Retrieves all Datasets of a project from the Platform Repository.
   *
   * @param project_id The project ID whose Datasets are to be retrieved.
   * @return The list of all Datasets for the given project, empty list if there are no Datasets.
   */
  def getAllDatasets(project_id: String): Future[Seq[Dataset]] = {
    db.getCollection[Dataset](COLLECTION_NAME).find(equal("project_id", project_id)).toFuture() flatMap { datasets =>
      Future.sequence(
        // Ask the data preparation results for each dataset to their DatasetSources
        // Do this job in pararllel and then join with Future.sequence to return a Future[Seq[Dataset]]
        datasets.map(dataset => FederatedQueryManager.askAgentsDataPreparationResults(dataset))
      )
    }
  }

  /**
   * Updates the Dataset by doing a replacement.
   *
   * @param dataset The Dataset object to be updated.
   * @return The updated Dataset object if operation is successful, None otherwise.
   */
  def updateDataset(dataset: Dataset): Future[Option[Dataset]] = {
    // TODO: Add some integrity checks before document replacement
    // Update the execution state of the dataset to "final"
    val datasetWithNewExecutionState = dataset.withExecutionState(ExecutionState.FINAL)

    db.getCollection[Dataset](COLLECTION_NAME).findOneAndReplace(
      equal("dataset_id", datasetWithNewExecutionState.dataset_id.get),
      datasetWithNewExecutionState,
      FindOneAndReplaceOptions().returnDocument(ReturnDocument.AFTER)).headOption()
  }

  /**
   * Deletes Dataset from the Platform Repository.
   *
   * @param dataset_id The unique identifier of the Dataset to be deleted.
   * @return The deleted Dataset object if operation is successful, None otherwise.
   */
  def deleteDataset(dataset_id: String): Future[Option[Dataset]] = {
    db.getCollection[Dataset](COLLECTION_NAME).findOneAndDelete(equal("dataset_id", dataset_id)).headOption()
  }
}

