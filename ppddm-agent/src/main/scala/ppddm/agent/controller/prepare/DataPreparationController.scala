package ppddm.agent.controller.prepare

import akka.actor.ActorSystem
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame
import ppddm.core.rest.model.{DataPreparationRequest, DataPreparationResult, Featureset}

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

  implicit val system = ActorSystem()

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
    val eligiblePatients = QueryHandler.executeEligibilityQuery(dataPreparationRequest.eligibility_criteria)
    // TODO: 2. Create a DataFrame from the results of step 1.
    // TODO: 3. Apply a set of transformations on the DataFrame of step 2 to come up with the final DataFrame which
    //  will correspond to the extracted/prepared data
    // TODO: 4. Create the DataPreparationResult for the given dataset_id and create a DataFrame containing this information.
    //  Cache this DataFrame and also persist (save as another parquet file). While the Agent is starting up, it will read these kind of
    //  DataFrames and cache.
    Future {  }
  }

  /**
   * Creates a DataFrame out of the given patient identifiers and then applies a series of transformations to
   * end up with a DataFrame that corresponds to the tabular data in which columns are the variables of the Featureset and
   * rows are values extracted from the FHIR Repository corresponding those variables for each patient.
   *
   * @param featureset
   * @param patientIds
   * @return
   */
  private def prepareFeaturesetValues(featureset: Featureset, patientIds: Seq[String]): Future[DataFrame] = {
    // For implicit conversions from RDDs to DataFrames
    //import spark.implicits._
    null
  }

  def getDataSourceStatistics(dataset_id: String): Future[Option[DataPreparationResult]] = {
    Future {
      None
    }
  }

}
