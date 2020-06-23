package ppddm.manager.controller.dataset

import java.util.UUID

import com.typesafe.scalalogging.Logger
import ppddm.manager.Manager
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates.{combine, set}
import ppddm.core.exception.{DBException, NotFoundException}
import ppddm.core.rest.model.{DataSource, DataSourceStatus, Dataset}

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
    // TODO 1. Fetch data sources from Service Registry

    // 2. Create DataSource object for each data source in QUERYING status.
    val dataSources = Seq[DataSource](
      DataSource(Some(UUID.randomUUID().toString), "SAS", DataSourceStatus.QUERYING, "/", None, None),
      DataSource(Some(UUID.randomUUID().toString), "UNIGE", DataSourceStatus.QUERYING, "/", None, None),
      DataSource(Some(UUID.randomUUID().toString), "IACS", DataSourceStatus.QUERYING, "/", None, None),
      DataSource(Some(UUID.randomUUID().toString), "UCSC", DataSourceStatus.QUERYING, "/", None, None),
      DataSource(Some(UUID.randomUUID().toString), "UP", DataSourceStatus.QUERYING, "/", None, None)
    ) // TODO to be deleted

    // 3. Save dataset to Platform Repository in QUERYING status.
    val datasetWithId = dataset.withUniqueDatasetId // Create a new Dataset object with a unique identifier
      .withStatus(DataSourceStatus.QUERYING) // in QUERYING status
      .withDataSources(dataSources) // with data sources
    db.getCollection[Dataset](COLLECTION_NAME).insertOne(datasetWithId).toFuture() // insert into the database
      .map { result =>
        val _id = result.getInsertedId.asObjectId().getValue.toString
        logger.debug("Inserted document _id:{} and datasetId:{}", _id, datasetWithId.dataset_id.get)
        // TODO invoke agents to start data extraction process
        datasetWithId
      }
      .recoverWith {
        case e: Exception =>
          val msg = s"Error while inserting a Dataset with dataset_id:${datasetWithId.dataset_id.get} into the database."
          logger.error(msg, datasetWithId.dataset_id.get, e)
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
    db.getCollection[Dataset](COLLECTION_NAME).find().toFuture()
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

