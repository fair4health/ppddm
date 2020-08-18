package ppddm.agent.controller.prepare

import com.typesafe.scalalogging.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.hl7.fhir.r4.model.{Condition, DomainResource, Observation, Patient, Resource}
import ppddm.agent.Agent
import ppddm.agent.config.AgentConfig
import ppddm.core.fhir.{FHIRClient, FHIRQuery, FHIRQueryWithParams, FHIRQueryWithQueryString}
import ppddm.core.rest.model.EligibilityCriteria

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.Try

/**
 * Handles the Queries on the HL7 FHIR Repository
 */
object QueryHandler {

  private val logger: Logger = Logger(this.getClass)

  private val batchSize: Int = AgentConfig.agentBatchSize
  private val sparkSession: SparkSession = Agent.dataMiningEngine.sparkSession
  private val fhirClient: FHIRClient = Agent.fhirClient

  /**
   * Retrieves the identifiers of the eligible patients from the FHIR Repository
   *
   * @param eligibilityCriteria
   */
  def executeEligibilityQuery(eligibilityCriteria: Seq[EligibilityCriteria]): Future[Seq[String]] = {
    logger.debug(s"Will execute the eligibility query: ${eligibilityCriteria}")

    // Start with the Patients
    // TODO: What if there are multiple Patient queries in the eligibilityCriteria
    val patientEC = eligibilityCriteria.find(_.fhir_query.startsWith("/Patient"))
    val patientQuery = if (patientEC.isDefined) {
      FHIRQueryWithQueryString(patientEC.get.fhir_query)
    } else {
      FHIRQueryWithQueryString("/Patient")
    }
    val numOfResults: Int = patientQuery.getCount(fhirClient.getClient()) //Count the resulting resources
    logger.debug(s"Number of patients: ${numOfResults}")
    if (numOfResults > 0) {
      //Number of pages to get all the results according to batch size
      val numOfReturnPagesForQuery = numOfResults / batchSize + 1
      logger.debug(s"Number of workers to be run in parallel: ${numOfReturnPagesForQuery}")
      //Parallelize the execution and get pages in parallel
      val rdd = sparkSession.sparkContext.parallelize(1 to numOfReturnPagesForQuery).mapPartitions(partitionIterator => {
        val resources = partitionIterator.map { pageIndex =>
          // Fetch the Patient resources from the FHIR Repository and collect their IDs
          val patientIDs: Seq[String] = Try(patientQuery.getResources[Patient](fhirClient.getClient(), batchSize, pageIndex)).recover {
            case t: Throwable =>
              logger.error("Problem in FHIR query while retrieving patients", t)
              Nil
          }.get
            .map(patient => patient.getIdElement.getIdPart)

          logger.debug(s"Finding eligible patients at page index ${pageIndex}")

          // Fetch the remaining resources (in parallel) indicated within the eligibilityCriteria
          val queryFutures = eligibilityCriteria.filterNot(_.fhir_query.startsWith("/Patient")).map {
            case conditionEC if conditionEC.fhir_query.startsWith("/Condition") => findEligiblePatients[Condition](patientIDs, conditionEC)
            case observationEC if observationEC.fhir_query.startsWith("/Observation") => findEligiblePatients[Observation](patientIDs, observationEC)
          }

          if (queryFutures.isEmpty) { // It means there is eligibility criteria other than the one(s) related to Patient
            Future { patientIDs }
              .map(res => {
                logger.debug(s"${res.size} eligible patients are found at page index ${pageIndex}.") // Log the result
                res
              })
          } else { // There are other eligibility criteria(s)
            Future.sequence(queryFutures) // Join the parallel Futures
              .map(_.flatten.distinct) // Merge the sequence of patientIds into a single sequence and then eliminate the duplicates
              .map(res => {
                logger.debug(s"${res.size} eligible patients are found at page index ${pageIndex}.") // Log the result
                res
              })
          }
        }
        resources
      })

      rdd.collect() // RDDs are lazy evaluated. In order to materialize the above statement, call an action rdd such as foreach, collect, count etc.
      // TODO If you are going to use the same RDD more than once, make sure to call rdd.cache() first. Otherwise, it will be executed in each action
      // TODO When you are done, call rdd.unpersist() to remove it from cache.

    } else {
      sparkSession.sparkContext.parallelize(Nil)
    }

    //Agent.fhirClient.runDistributedFHIRQuery[Patient](Agent.dataMiningEngine.sparkSession, fhirQuery).mapPartitions[Patient]()
    null

  }

  /**
   * Fetch the resources according to the eligibilityCriteria for the given list of patients.
   *
   * @param patientIDs
   * @param eligibilityCriteria
   * @return
   */
  private def findEligiblePatients[T <: DomainResource](patientIDs: Seq[String], eligibilityCriteria: EligibilityCriteria)(implicit m: Manifest[T]): Future[Seq[String]] = {
    Future {
      val patientURIs = patientIDs.map(pid => s"Patient/$pid") // Prepend the Patient keyword to each pid

      val queryString: String = // Create the queryString for FHIR Query
        if (eligibilityCriteria.fhir_query.contains("?"))
          s"${eligibilityCriteria.fhir_query}&subject=${patientURIs.mkString(",")}"
        else s"${eligibilityCriteria.fhir_query}?subject=${patientURIs.mkString(",")}"

      val fhirQuery = FHIRQueryWithQueryString(queryString)

      val resources: Seq[T] = Try(fhirQuery.getResources[T](fhirClient.getClient())).recover {
        case t: Throwable =>
          logger.error("Problem in FHIR query while retrieving resources", t)
          Nil
      }.get

      resources.map {
        case condition: Condition => condition.getSubject.getReference
        case observation: Observation => observation.getSubject.getReference
      }
    }

  }

  //  private def findEligiblePatients[T <: Resource](eligibilityCriteria: EligibilityCriteria)(implicit m: Manifest[T]): Future[Seq[String]] = {
  //    val fhirClient = Agent.fhirClient.getClient() // Get a HAPI FHIR Client
  //    val numOfResults: Int = query.getCount(fhirClient) //Count the resulting resources
  //    if (numOfResults > 0) {
  //      //Number of pages to get all the results according to batch size
  //      val numOfReturnPagesForQuery = numOfResults / batchSize + 1
  //      //Parallelize the execution and get pages in parallel
  //      sparkSession.sparkContext.parallelize(1 to numOfReturnPagesForQuery).mapPartitions[T](partitionPages => {
  //        val fhirClient = getClient()
  //        val resources = partitionPages.flatMap { pageIndex =>
  //          Try(query.getResources[T](fhirClient, batchSize, pageIndex)).recover {
  //            case t: Throwable =>
  //              logger.error("Problem in FHIR query: getResources", t)
  //              Nil
  //          }.get
  //        }
  //        resources
  //      })
  //    } else sparkSession.sparkContext.parallelize(Nil)
  //  }

  /*private def findEligiblePatients[T <: Resource](eligibilityCriteria: EligibilityCriteria)(implicit m: Manifest[T]): Future[Seq[String]] = {
    Agent.fhirClient.query[T](eligibilityCriteria.fhir_query) map { bundle =>
      val patientIds = ListBuffer.empty[String]
      if (bundle.entry.isDefined) {
        bundle.entry.get foreach { entry =>
          if (entry.resource.isDefined) {
            entry.resource.get match {
              case patient: Patient => patient.id.map(pid => patientIds += pid)
              case condition: Condition => condition.subject.reference.map(pid => patientIds += pid)
              case observation: Observation => observation.subject.map(_.reference.map(pid => patientIds += pid))
            }
          }
        }
      }
      patientIds.toList
    }

    /*
    val query = FhirQuery("Patient", Nil)
    val patients:RDD[org.hl7.fhir.r4.model.Patient] =
      Agent.fhirRestSource.runDistributedFhirQuery[org.hl7.fhir.r4.model.Patient](Agent.dataMiningEngine.sparkSession, query)(classTag[org.hl7.fhir.r4.model.Patient])
    */

  }*/
}
