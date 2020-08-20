package ppddm.agent.controller.prepare

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.Logger
import io.onfhir.path.FhirPathEvaluator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s.JString
import ppddm.agent.Agent
import ppddm.agent.config.AgentConfig
import ppddm.core.fhir.FHIRClient
import ppddm.agent.spark.NodeExecutionContext._
import ppddm.core.rest.model.{DataPreparationRequest, DataPreparationResult, EligibilityCriterion}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, TimeoutException}

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

  /**
   * Start the data preparation (data extraction process) with the given DataPreparationRequest.
   * This function successfully returns if the preparation request is started. Data preparation will continue in the background
   * and the clients will ask about the execution status through a separate API call.
   *
   * @param dataPreparationRequest The request object for the data preparation.
   * @return
   */
  def startPreparation(dataPreparationRequest: DataPreparationRequest): Future[Set[String]] = {
    // TODO: We may need some validation on the DataPreparationRequest object
    prepareData(dataPreparationRequest)
  }

  private def prepareData(dataPreparationRequest: DataPreparationRequest): Future[Set[String]] = {
    logger.debug("Data preparation request received.")

    val fhirClientMaster = FHIRClient(AgentConfig.fhirHost, AgentConfig.fhirPort, AgentConfig.fhirPath, AgentConfig.fhirProtocol)

    // Start with the Patients
    val patientQuery = QueryHandler.getPatientQuery(dataPreparationRequest.eligibility_criteria)

    patientQuery.getCount(fhirClientMaster) map { numOfPatients => //Count the resulting resources
      logger.debug(s"Number of patients: ${numOfPatients}")
      if (numOfPatients > 0) {
        //Number of pages to get all the results according to batch size
        val numOfReturnPagesForQuery = numOfPatients / batchSize + 1
        logger.debug(s"Number of workers to be run in parallel in Spark: ${numOfReturnPagesForQuery}")

        //Parallelize the execution and process pages in parallel
        val rdd: RDD[Set[String]] = sparkSession.sparkContext.parallelize(1 to numOfReturnPagesForQuery).mapPartitions(partitionIterator => {
          partitionIterator.map { pageIndex =>
            val fhirClientPartition = FHIRClient(AgentConfig.fhirHost, AgentConfig.fhirPort, AgentConfig.fhirPath, AgentConfig.fhirProtocol)
            val fhirPathEvaluator = FhirPathEvaluator()

            // Fetch the Patient resources from the FHIR Repository and collect their IDs
            val theFuture = patientQuery.getResources(fhirClientPartition, batchSize, pageIndex) flatMap { results =>

              // If fhir_path is non-empty, filter resulting patients which satisfy the FHIR path expression
              val patients = results.filter(patientQuery.getFHIRPath().isEmpty || fhirPathEvaluator.satisfies(patientQuery.getFHIRPath().get, _))

              val patientURIs: Set[String] = patients
                .map(p => (p \ "id").asInstanceOf[JString].values) // extract the IDs from the JObject of Patient
                .map(pid => s"Patient/$pid") // Prepend the Patient keyword to each pid
                .toSet[String] // Convert to a set

              if (patientURIs.isEmpty) {
                // There are no eligible patients in this partition. We do not need to execute the remaining criteria on other resources
                logger.debug(s"There are no eligible patients in this partition at page index ${pageIndex}.") // Log the result
                Future {
                  Set.empty[String]
                }
              } else {
                logger.debug(s"Finding eligible patients at page index ${pageIndex}")

                // Fetch the remaining resources (in parallel within worker node) indicated within the eligibilityCriteria
                val criteriaOtherThanPatientResource = dataPreparationRequest.eligibility_criteria.filterNot(_.fhir_query.startsWith("/Patient"))
                if (criteriaOtherThanPatientResource.isEmpty) {
                  // There is only one criterion on the Patient resource, hence we need to use the already retrieved patientsIDs as the eligible patients
                  logger.debug(s"${patientURIs.size} eligible patients are found.") // Log the result
                  Future {
                    patientURIs
                  }
                } else {
                  // There are other resources in addition to the Patient resource query. Hence, they need to be executed to narrow down the eligible patients
                  val queryFutures = criteriaOtherThanPatientResource.map(findEligiblePatientIDs(fhirClientPartition, fhirPathEvaluator, patientURIs, _))

                  Future.sequence(queryFutures) // Join the parallel Futures
                    .map { queryResults =>
                      val result = queryResults.reduceLeft((a, b) => a.intersect(b)) // Create a final list bu intersecting the resulting lists
                      logger.debug(s"${result.size} eligible patients are found at page index ${pageIndex}.") // Log the result
                      result
                    }
                }
              }
            }
            try { // Wait for the eligibility criteria to be executed
              Await.result(theFuture, Duration(60, TimeUnit.SECONDS))
            } catch {
              case e: TimeoutException =>
                logger.error("The eligibility criteria cannot be executed on the FHIR Repository within 60 seconds", e)
                Set.empty[String] // TODO: Check whether we can do better than returning an empty sequence. What happens if a worker node throws an exception?
            }
          }
        })

        rdd.collect().toSet.flatten // RDDs are lazy evaluated. In order to materialize the above statement, call an action rdd such as foreach, collect, count etc.
        // TODO If you are going to use the same RDD more than once, make sure to call rdd.cache() first. Otherwise, it will be executed in each action
        // TODO When you are done, call rdd.unpersist() to remove it from cache.

      } else {
        logger.info("There are no patients for the given eligibility criteria: {}", dataPreparationRequest.eligibility_criteria)
        Set.empty[String]
      }
    }

  }

  /**
   * Given a list of patient URIs, find the resources of those patients according to the eligibilityCriteria defined for each resource
   * and then collect the subjects (patient IDs) of the resulting resources.
   * This will only retrieve the resources of the given patients. And then those patientIDs will be returned by this function.
   *
   * @param fhirClient
   * @param fhirPathEvaluator
   * @param patientURIs
   * @param eligibilityCriteria
   * @return
   */
  private def findEligiblePatientIDs(fhirClient: FHIRClient, fhirPathEvaluator: FhirPathEvaluator, patientURIs: Set[String],
                                     eligibilityCriteria: EligibilityCriterion): Future[Set[String]] = {
    val fhirQuery = QueryHandler.getResoucesOfPatientsQuery(patientURIs, eligibilityCriteria)
    fhirQuery.getResources(fhirClient) map { resources =>
      resources
        .filter(eligibilityCriteria.fhir_path.isEmpty // If fhir_path is non-empty, check if the given resource satisfies the FHIR path expression
          || fhirPathEvaluator.satisfies(eligibilityCriteria.fhir_path.get, _))
        .map(r => (r \ "subject" \ "reference").asInstanceOf[JString].values) // Collect the patientIDs
        .toSet // Convert to set
    }
  }

  def getDataSourceStatistics(dataset_id: String): Future[Option[DataPreparationResult]] = {
    Future {
      None
    }
  }

}
