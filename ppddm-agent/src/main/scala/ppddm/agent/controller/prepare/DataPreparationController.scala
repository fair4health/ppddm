package ppddm.agent.controller.prepare

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.hl7.fhir.r4.model.{Condition, DomainResource, Observation, Patient}
import ppddm.agent.Agent
import ppddm.agent.config.AgentConfig
import ppddm.core.fhir.FHIRClient
import ppddm.core.rest.model.{DataPreparationRequest, DataPreparationResult, EligibilityCriterion}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, TimeoutException}
import scala.util.Try

/**
 * Controller object for data preparation.
 *
 * PPDDM Manager requests data preparation together with the unique identifier of the dataset on the platform.
 * After the submission, the manager checks whether the data is prepared or not by asking for the results.
 */
object DataPreparationController {

  private val logger: Logger = Logger(this.getClass)

  private val batchSize: Int = AgentConfig.agentBatchSize
  private val sparkSession: SparkSession = Agent.dataMiningEngine.sparkSession
  private val fhirClient: FHIRClient = Agent.fhirClient

  /**
   * Start the data preparation (data extraction process) with the given DataPreparationRequest.
   * This function successfully returns if the preparation request is started. Data preparation will continue in the background
   * and the clients will ask about the execution status through a separate API call.
   *
   * @param dataPreparationRequest The request object for the data preparation.
   * @return
   */
  def startPreparation(dataPreparationRequest: DataPreparationRequest): Future[Seq[String]] = {
    // TODO: We may need some validation on the DataPreparationRequest object
    Future {
      prepareData(dataPreparationRequest)
    }
  }

  private def prepareData(dataPreparationRequest: DataPreparationRequest): Seq[String] = {
    logger.debug("Data preparation request received.")

    // Start with the Patients
    val patientQuery = QueryHandler.getPatientQuery(dataPreparationRequest.eligibility_criteria)

    val numOfPatients: Int = patientQuery.getCount(fhirClient.getClient()) //Count the resulting resources
    logger.debug(s"Number of patients: ${numOfPatients}")

    if (numOfPatients > 0) {
      //Number of pages to get all the results according to batch size
      val numOfReturnPagesForQuery = numOfPatients / batchSize + 1
      logger.debug(s"Number of workers to be run in parallel in Spark: ${numOfReturnPagesForQuery}")

      //Parallelize the execution and process pages in parallel
      val rdd: RDD[Seq[String]] = sparkSession.sparkContext.parallelize(1 to numOfReturnPagesForQuery).mapPartitions(partitionIterator => {
        partitionIterator.map { pageIndex =>
          // Fetch the Patient resources from the FHIR Repository and collect their IDs
          val patientURIs: Seq[String] = Try(patientQuery.getResources[Patient](fhirClient.getClient(), batchSize, pageIndex)).recover {
            case t: Throwable =>
              logger.error("Problem in FHIR query while retrieving patients", t)
              Nil
          }.get
            .map(patient => patient.getIdElement.getIdPart)
            .map(pid => s"Patient/$pid") // Prepend the Patient keyword to each pid

          if (patientURIs.isEmpty) {
            // There are no eligible patients in this partition. We do not need to execute the remaining criteria on other resources
            logger.debug(s"There are no eligible patients in this partition at page index ${pageIndex}.") // Log the result
            Seq.empty
          } else {

            logger.debug(s"Finding eligible patients at page index ${pageIndex}")

            // Fetch the remaining resources (in parallel within worker node) indicated within the eligibilityCriteria
            val criteriaOtherThanPatientResource = dataPreparationRequest.eligibility_criteria.filterNot(_.fhir_query.startsWith("/Patient"))
            if (criteriaOtherThanPatientResource.isEmpty) {
              // There is only one criterion on the Patient resource, hence we need to use the already retrieved patientsIDs as the eligible patients
              logger.debug(s"${patientURIs.size} eligible patients are found.") // Log the result
              patientURIs
            } else {
              // There are other resources in addition to the Patient resource query. Hence, they need to be executed to narrow down the eligible patients
              val queryFutures = criteriaOtherThanPatientResource.map {
                case conditionEC if conditionEC.fhir_query.startsWith("/Condition") => findEligiblePatientIDs[Condition](patientURIs, conditionEC)
                case observationEC if observationEC.fhir_query.startsWith("/Observation") => findEligiblePatientIDs[Observation](patientURIs, observationEC)
              }
              val theFuture = Future.sequence(queryFutures) // Join the parallel Futures
                .map(_.flatten.distinct) // Merge the sequence of patientIds into a single sequence and then eliminate the duplicates
                .map(res => {
                  logger.debug(s"${res.size} eligible patients are found at page index ${pageIndex}.") // Log the result
                  res
                })
              try { // Wait for the eligibility criteria to be executed
                Await.result(theFuture, Duration(60, TimeUnit.SECONDS))
              } catch {
                case e: TimeoutException =>
                  logger.error("The eligibility criteria cannot be executed on the FHIR Repository within 60 seconds", e)
                  Seq.empty // TODO: Check whether we can do better than returning an empty sequence. What happens if a worker node throws an exception?
              }
            }
          }
        }
      })
      rdd.collect().toSeq.flatten // RDDs are lazy evaluated. In order to materialize the above statement, call an action rdd such as foreach, collect, count etc.
      // TODO If you are going to use the same RDD more than once, make sure to call rdd.cache() first. Otherwise, it will be executed in each action
      // TODO When you are done, call rdd.unpersist() to remove it from cache.

    } else {
      logger.info("There are no patients for the given eligibility criteria: {}", dataPreparationRequest.eligibility_criteria)
      Seq.empty
    }

    // TODO: 1. Execute the eligibility query on FHIR Repository to retrieve the final set of resources to be used for data extraction
    // TODO: 2. Create a DataFrame from the results of step 1.
    // TODO: 3. Apply a set of transformations on the DataFrame of step 2 to come up with the final DataFrame which
    //  will correspond to the extracted/prepared data
    // TODO: 4. Create the DataPreparationResult for the given dataset_id and create a DataFrame containing this information.
    //  Cache this DataFrame and also persist (save as another parquet file). While the Agent is starting up, it will read these kind of
    //  DataFrames and cache.
  }

  /**
   * Given a list of patient URIs, find the resources of those patients according to the eligibilityCriteria defined for each resource
   * and then collect the subjects (patient IDs) of the resulting resources.
   * This will only retrieve the resources of the given patients. And then those patientIDs will be returned by this function.
   *
   * @param patientURIs
   * @param eligibilityCriteria
   * @return
   */
  private def findEligiblePatientIDs[T <: DomainResource](patientURIs: Seq[String], eligibilityCriteria: EligibilityCriterion)(implicit m: Manifest[T]): Future[Seq[String]] = {
    // TODO: Add the FHIRPath evaluation into this function.

    Future {
      val fhirQuery = QueryHandler.getResoucesOfPatientsQuery(patientURIs, eligibilityCriteria)

      val resources: Seq[T] = Try(fhirQuery.getResources[T](fhirClient.getClient())).recover {
        case t: Throwable =>
          logger.error("Problem in FHIR query while retrieving resources of the given patients", t)
          Nil
      }.get

      resources.map {
        case condition: Condition => condition.getSubject.getReference
        case observation: Observation => observation.getSubject.getReference
      }
    }

  }

  def getDataSourceStatistics(dataset_id: String): Future[Option[DataPreparationResult]] = {
    Future {
      None
    }
  }

}
