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
 * Controller object for the Datasets. Dataset extracted data from FHIR based on the specifications defined in Featureset and eligibility criteria.
 */
object DatasetController {

  val COLLECTION_NAME: String = "dataset"

  private val logger: Logger = Logger(this.getClass)
  private val db = Manager.mongoDB.getDatabase

  /**
   * Saves the data set object containing only name, description and eligibility criteria to the Platform Repository and invokes agents to start data extraction process.
   * @param dataset The data set object containing only name, description and eligibility criteria.
   * @return The data set object with a unique identifier and created data sources if operation is successful, error otherwise.
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
                               .withDataSources(dataSources)  // with data sources
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
   * Retrieves all the feature sets from the Platform Repository.
   * @return empty list if there is no feature set in the repository, the list of datasets otherwise.
   */
  def getAllDatasets(): Future[Seq[Dataset]] = {
    db.getCollection[Dataset](COLLECTION_NAME).find().toFuture()
  }

  /**
   * Retrieves the feature set with specific unique identifier from the Platform Repository.
   * @param dataset_id The unique identifier of the feature set to be returned
   * @return null if dataset_id is not valid, the Dataset object otherwise.
   */
  def getDataset(dataset_id: String): Future[Option[Dataset]] = {
    db.getCollection[Dataset](COLLECTION_NAME).find(equal("dataset_id", dataset_id)).first().headOption()
  }

  /**
   * Updates the feature set.
   * @param dataset The Dataset object to be updated.
   * @return the updated Dataset object if operation is successful, error otherwise.
   */
  def updateDataset(dataset: Dataset): Future[Dataset] = { // TODO to be updated
    db.getCollection[Dataset](COLLECTION_NAME).findOneAndUpdate(
      equal("dataset_id", dataset.dataset_id.get),
      combine(set("name", dataset.name), set("description", dataset.description))
    ).toFuture()
  }

  /**
   * Deletes dataset from the Platform Repository.
   * @param dataset_id The unique identifier of the dataset to be deleted.
   * @return the number of deleted records if operation is successful, error otherwise.
   */
  def deleteDataset(dataset_id: String): Future[Long] = {
    db.getCollection[Dataset](COLLECTION_NAME).deleteOne(equal("dataset_id", dataset_id)).toFuture() map { result =>
      val count = result.getDeletedCount
      if (count == 0) {
        throw NotFoundException(s"The Dataset with id:${dataset_id} not found")
      }
      count
    }
  }
}

