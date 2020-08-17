package ppddm.agent.controller.prepare

import com.typesafe.scalalogging.Logger
import org.apache.spark.rdd.RDD
import ppddm.agent.Agent
import ppddm.core.fhir.FhirQuery
import ppddm.core.fhir.r4.resources.{Bundle, Condition, Observation, Patient, Resource}
import ppddm.core.rest.model.EligibilityCriteria

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.classTag

/**
 * Handles the Queries on the HL7 FHIR Repository
 */
object QueryHandler {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Retrieves the identifiers of the eligible patients from the FHIR Repository
   *
   * @param eligibilityCriteria
   */
  def executeEligibilityQuery(eligibilityCriteria: Seq[EligibilityCriteria]): Future[Seq[String]] = {
    logger.debug("Will execute the eligibility query: {}", eligibilityCriteria)

    val queryFutures = eligibilityCriteria.map {
      case patientEC if patientEC.fhir_query.startsWith("/Patient") => findEligiblePatients[Patient](patientEC)
      case conditionEC if conditionEC.fhir_query.startsWith("/Condition") => findEligiblePatients[Condition](conditionEC)
      case observationEC if observationEC.fhir_query.startsWith("/Observation") => findEligiblePatients[Observation](observationEC)
    }

    Future.sequence(queryFutures) // Join the parallel Futures
      .map(_.flatten.distinct) // Merge the sequence of patientIds into a single sequence and then eliminate the duplicates
      .map(res => {
        logger.debug(s"${res.size} number of eligible patients are found.") // Log the result
        res
      })
  }

  private def findEligiblePatients[T <: Resource](eligibilityCriteria: EligibilityCriteria)(implicit m: Manifest[T]): Future[Seq[String]] = {
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

  }
}
