package ppddm.agent.controller.prepare

import com.typesafe.scalalogging.Logger
import ppddm.core.rest.model.{DataPreparationRequest, DataPreparationResult}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Controller object for data preparation.
 *
 * PPDDM Manager requests data preparation together with the unique identifier of the dataset on the platform.
 * After the submission, the manager checks whether the data is prepared or not by asking for the results.
 */
object DataPreparationController {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Start the data preparation (data extraction process) with the given DataPreparationRequest.
   * This function successfully returns if the preparation request is started.
   *
   * @param dataPreparationRequest The request object for the data preparation.
   * @return
   */
  def startPreparation(dataPreparationRequest: DataPreparationRequest): Future[Unit] = {
    // TODO: We may need some validation on the DataPreparationRequest object
    prepareData(dataPreparationRequest)
  }

  private def prepareData(dataPreparationRequest: DataPreparationRequest): Future[Unit] = {
    // TODO: 1. Execute the eligibility query on FHIR Repository to retrieve the final set of resources to be used for data extraction
    // TODO: 2. Create a DataFrame from the results of step 1.
    // TODO: 3. Apply a set of transformations on the DataFrame of step 2 to come up with the final DataFrame which
    //  will corespond to the extracted/prepared data
    // TODO: 4. Create the DataPreparationResult for the given dataset_id and create a DataFrame containing this information.
    //  Cache this DataFrame and also persist (save as another parquet file). While the Agent is starting up, it will read these kind of
    //  DataFrames and cache.
    Future {}
  }

  def getDataSourceStatistics(dataset_id: String): Future[Option[DataPreparationResult]] = {
    Future {
      None
    }
  }

}
