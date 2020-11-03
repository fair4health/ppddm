package ppddm.manager.controller.dataset

import com.typesafe.scalalogging.Logger
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.{FindOneAndReplaceOptions, ReturnDocument}
import ppddm.core.exception.DBException
import ppddm.core.rest.model.{Dataset, DatasetSource, ExecutionState}
import ppddm.manager.Manager

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
    // Returns a new Dataset object with data sources and execution state "EXECUTING"
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
      .recover {
        case e: Exception =>
          val msg = s"Error while retrieving a Dataset with dataset_id:${dataset_id} from the database."
          throw DBException(msg, e)
      }
      .flatMap { datasetOption =>
        if (datasetOption.isDefined) { // If the dataset is found with the given dataset_id
          // Ask the data preparation results to its DatasetSources
          FederatedQueryManager.askAgentsDataPreparationResults(datasetOption.get) flatMap { dataset =>
            // Save the new Dataset to the database so that in the next call to this function
            // I do not ask the results to the agents which have already in their FINAL states
            updateDataset(dataset)
          }
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
    db.getCollection[Dataset](COLLECTION_NAME).find(equal("project_id", project_id)).toFuture()
      .recover {
        case e: Exception =>
          val msg = s"Error while retrieving the Datasets of the Project with project_id:${project_id} from the database."
          throw DBException(msg, e)
      }
      .flatMap { datasets =>
        Future.sequence(
          // Ask the data preparation results for each dataset to their DatasetSources
          // Do this job in parallel and then join with Future.sequence to return a Future[Seq[Dataset]]
          datasets.map(dataset => FederatedQueryManager.askAgentsDataPreparationResults(dataset) flatMap { datasetWithNewDataSources =>
            // Save the new Dataset to the database so that in the next call to this function
            // I do not ask the results to the agents which have already in their FINAL states
            updateDataset(datasetWithNewDataSources) map { updatedDataset =>
              if (updatedDataset.isDefined) updatedDataset.get
              else datasetWithNewDataSources
            }
          })
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
    // TODO: Check whether at least one DatasetSource is selected
    db.getCollection[Dataset](COLLECTION_NAME).findOneAndReplace(
      equal("dataset_id", dataset.dataset_id.get),
      dataset.withUpdatedExecutionState(),
      FindOneAndReplaceOptions().returnDocument(ReturnDocument.AFTER))
      .headOption()
      .recover {
        case e: Exception =>
          val msg = s"Error while updating the Dataset with dataset_id:${dataset.dataset_id.get} in the database."
          throw DBException(msg, e)
      }
  }

  /**
   * Deletes Dataset from the Platform Repository.
   *
   * @param dataset_id The unique identifier of the Dataset to be deleted.
   * @return The deleted Dataset object if operation is successful, None otherwise.
   */
  def deleteDataset(dataset_id: String): Future[Option[Dataset]] = {
    db.getCollection[Dataset](COLLECTION_NAME).findOneAndDelete(equal("dataset_id", dataset_id))
      .headOption()
      .recover {
        case e: Exception =>
          val msg = s"Error while deleting the Dataset with dataset_id:${dataset_id} from the database."
          throw DBException(msg, e)
      }
      .flatMap { datasetOption: Option[Dataset] =>
        if (datasetOption.isDefined) {
          val dataset = datasetOption.get
          Future.sequence(
            dataset.dataset_sources.get.map { datasetSource: DatasetSource => // For each DataSource in this set
              FederatedQueryManager.deleteDatasetAndStatistics(datasetSource.agent, dataset) // Delete the extracted datasets and statistics from the Agents (do this in parallel)
            }) map { _ => Some(dataset) }
        }
        else {
          Future.apply(Option.empty[Dataset])
        }
      }
  }

}

