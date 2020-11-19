package ppddm.manager.controller.dataset

import com.typesafe.scalalogging.Logger
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.{FindOneAndReplaceOptions, ReturnDocument}
import ppddm.core.exception.DBException
import ppddm.core.rest.model.{Dataset, DatasetSource, ExecutionState, SelectionStatus}
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
  def createDataset(dataset: Dataset, isTest: Boolean = false): Future[Dataset] = {

    if(dataset.dataset_id.isDefined) {
      throw new IllegalArgumentException("If you want to create a new data set, please provide it WITHOUT a dataset_id")
    }
    if(dataset.featureset.variables.isEmpty) {
      logger.error("How does this happen? The client sends a Dataset creation request, but there is no variable inside the featureset within this dataset definition")
      throw new IllegalArgumentException("A feature set must include at least one variable")
    }

    // Create a new Dataset object with a unique identifier
    val datasetWithId = dataset.withUniqueDatasetId

    if (isTest) {
      logger.debug("This Dataset creation is for testing. Dataset will only be saved into the MongoDB database, no Agents will be invoked.")
      saveDatasetToDB(datasetWithId)
    } else {
      // Invoke agents to start data preparation processes
      // Returns a new Dataset object with data sources and execution state "EXECUTING"
      FederatedQueryManager.invokeAgentsDataPreparation(datasetWithId) flatMap { datasetWithDataSources =>
        logger.info("Data preparation endpoints are invoked for all registered Agents (datasources)")
        saveDatasetToDB(datasetWithDataSources)
      }
    }
  }

  /**
   * Persists the given dataset into the MongoDB database
   *
   * @param dataset
   * @return
   */
  private def saveDatasetToDB(dataset: Dataset): Future[Dataset] = {
    db.getCollection[Dataset](COLLECTION_NAME).insertOne(dataset).toFuture() // insert into the database
      .map { result =>
        val _id = result.getInsertedId.asObjectId().getValue.toString
        logger.debug("Inserted document _id:{} and datasetId:{}", _id, dataset.dataset_id.get)
        dataset
      }
      .recover {
        case e: Exception =>
          val msg = s"Error while inserting a Dataset with dataset_id:${dataset.dataset_id.get} into the database."
          throw DBException(msg, e)
      }
  }

  /**
   * Retrieves the Dataset from the Platform Repository.
   *
   * @param dataset_id The unique identifier of the Dataset
   * @return The Dataset if dataset_id is valid, None otherwise.
   */
  def getDataset(dataset_id: String, isTest: Boolean = false): Future[Option[Dataset]] = {
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
          if(isTest) { // If this is a test call, then do not invoke the Agents
            logger.debug("This Dataset retrieval is for testing. Dataset will only be retrieved from the MongoDB database, no Agents will be invoked.")
            Future.apply(Some(datasetOption.get))
          } else {
            // Ask the data preparation results to its DatasetSources
            FederatedQueryManager.askAgentsDataPreparationResults(datasetOption.get) flatMap { dataset =>
              // Save the new Dataset to the database so that in the next call to this function
              // I do not ask the results to the agents which have already in their FINAL states
              updateDataset(dataset)
            }
          }
        } else {
          Future.apply(None)
        }
      }
  }

  /**
   * Retrieves all Datasets of a project from the Platform Repository.
   *
   * @param project_id The project ID whose Datasets are to be retrieved.
   * @return The list of all Datasets for the given project, empty list if there are no Datasets.
   */
  def getAllDatasets(project_id: String, isTest: Boolean = false): Future[Seq[Dataset]] = {
    db.getCollection[Dataset](COLLECTION_NAME).find(equal("project_id", project_id)).toFuture()
      .recover {
        case e: Exception =>
          val msg = s"Error while retrieving the Datasets of the Project with project_id:${project_id} from the database."
          throw DBException(msg, e)
      }
      .flatMap { datasets =>
        if (isTest) {
          logger.debug("This Dataset retrieval is for testing. All Datasets will only be retrieved from the MongoDB database, no Agents will be invoked.")
          Future.apply(datasets)
        }
        else {
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
  }

  /**
   * Updates the Dataset by doing a replacement.
   *
   * @param dataset The Dataset object to be updated.
   * @return The updated Dataset object if operation is successful, None otherwise.
   */
  def updateDataset(dataset: Dataset, isTest: Boolean = false): Future[Option[Dataset]] = {
    if(dataset.dataset_sources.isEmpty || dataset.dataset_sources.get.isEmpty) {
      throw new IllegalArgumentException(s"A data set must include at least one data set source at this point (while updating it). dataset_id:${dataset.dataset_id.get}")
    }
    if(!dataset.dataset_sources.get.exists(dss => dss.selection_status.isDefined && dss.selection_status.get == SelectionStatus.SELECTED)) {
      throw new IllegalArgumentException(s"At lease one of the data set sources of this dataset must be SELECTED while updating it. dataset_id:${dataset.dataset_id.get}")
    }
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
  def deleteDataset(dataset_id: String, isTest: Boolean = false): Future[Option[Dataset]] = {
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
          if(isTest) { // If this is a test call, then do not invoke the Agents
            logger.debug("This Dataset deletion is for testing. Dataset will only be deleted from the MongoDB database, no Agents will be invoked.")
            Future.apply(Some(dataset))
          } else {
            Future.sequence(
              dataset.dataset_sources.get.map { datasetSource: DatasetSource => // For each DataSource in this set
                FederatedQueryManager.deleteDatasetAndStatistics(datasetSource.agent, dataset) // Delete the extracted datasets and statistics from the Agents (do this in parallel)
              }) map { _ => Some(dataset) }
          }
        }
        else {
          Future.apply(Option.empty[Dataset])
        }
      }
  }

}

