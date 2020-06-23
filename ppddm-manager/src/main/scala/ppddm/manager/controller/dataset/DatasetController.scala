package ppddm.manager.controller.dataset

import com.typesafe.scalalogging.Logger
import ppddm.manager.Manager
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates.{combine, set}
import ppddm.core.exception.DBException
import ppddm.core.rest.model.{DataSourceStatus, Dataset}
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
   * Creates a new Dataset on the Platform Repository and invokes the agents to start their data extraction processes.
   *
   * @param dataset The Dataset to be created
   * @return The created Dataset with a unique dataset_id in it
   */
  def createDataset(dataset: Dataset): Future[Dataset] = {
    // Create a new Dataset object with a unique identifier in QUERYING status
    val datasetWithId = dataset.withUniqueDatasetId
                               .withStatus(DataSourceStatus.QUERYING)

    // Invoke agents to start data extraction process
    val dataSources = FederatedQueryManager.invokeAgents(datasetWithId)

    // Create a new Dataset object with data sources
    val datasetWithDataSources = datasetWithId.withDataSources(dataSources)

    db.getCollection[Dataset](COLLECTION_NAME).insertOne(datasetWithDataSources).toFuture() // insert into the database
      .map { result =>
        val _id = result.getInsertedId.asObjectId().getValue.toString
        logger.debug("Inserted document _id:{} and datasetId:{}", _id, datasetWithDataSources.dataset_id.get)
        datasetWithDataSources
      }
      .recoverWith {
        case e: Exception =>
          val msg = s"Error while inserting a Dataset with dataset_id:${datasetWithDataSources.dataset_id.get} into the database."
          logger.error(msg, datasetWithDataSources.dataset_id.get, e)
          throw DBException(msg, e)
      }
  }

  /**
   * Retrieves the Dataset from the Platform Repository.
   *
   * @param dataset_id The unique identifier of the Dataset
   * @return The Dataset if dataset_id is valid, None otherwise.
   */
  def getDataset(dataset_id: String): Future[Option[Dataset]] = {
    db.getCollection[Dataset](COLLECTION_NAME).find(equal("dataset_id", dataset_id)).first().headOption()
  }

  /**
   * Retrieves all Datasets from the Platform Repository.
   *
   * @return The list of all Datasets in the Platform Repository, empty list if there are no Datasets.
   */
  def getAllDatasets(): Future[Seq[Dataset]] = {
    db.getCollection[Dataset](COLLECTION_NAME).find().toFuture() map { datasets => {
      // TODO define a future list to send all requests to agents in parallel

      datasets map { dataset =>
        if (dataset.data_sources isDefined) {
          dataset.data_sources.get map { dataSource =>
            if (dataSource.status == DataSourceStatus.QUERYING) {
              // TODO ask agent about the status of data extraction
              // TODO if agent returns the result, update data source
              // TODO update status of dataset if all the agents responded
            }
          }
        }
      }

      datasets
    }}
  }

  /**
   * Updates the Dataset. Only name and description fields of a Dataset can be updated.
   *
   * @param dataset The Dataset object to be updated.
   * @return The updated Dataset object if operation is successful, None otherwise.
   */
  def updateDataset(dataset: Dataset): Future[Option[Dataset]] = {
    db.getCollection[Dataset](COLLECTION_NAME).findOneAndUpdate(
      equal("dataset_id", dataset.dataset_id.get),
      combine(set("name", dataset.name), set("description", dataset.description))
    ).headOption()
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

