package ppddm.agent.controller.prepare

import akka.actor.ActorSystem
import com.typesafe.scalalogging.Logger
import ppddm.core.fhir.r4.resources.{Bundle, Condition, Observation, Patient}
import ppddm.core.fhir.r4.service.FHIRClient
import ppddm.core.rest.model.EligibilityCriteria

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Handles the Queries on the HL7 FHIR Repository
 */
object QueryHandler {

  private val logger: Logger = Logger(this.getClass)

  implicit val system = ActorSystem()

  /**
   * Retrieves the eligible patients from the FHIR Repository
   *
   * @param eligibilityCriteria
   */
  def executeEligibilityQuery(eligibilityCriteria: Seq[EligibilityCriteria]): Future[Seq[String]] = {
    print(eligibilityCriteria)

    var patientIds = List[String]()

    for (ec <- eligibilityCriteria) {
      ec match {
        case conditionEC if conditionEC.fhir_query.startsWith("/Condition") => { patientIds = patientIds ++ findEligiblePatientsInCondition(conditionEC) }
        case observationEC if observationEC.fhir_query.startsWith("/Observation") => { patientIds = patientIds ++ findEligiblePatientsInObservation(observationEC) }
      }
    }

    Future {patientIds.distinct.toSeq}
  }

  private def findEligiblePatientsInCondition(eligibilityCriteria: EligibilityCriteria): Seq[String] = {
    val fhirClient = FHIRClient()

    val patientIds = ListBuffer[String]()

    val bundle: Bundle[Condition] = fhirClient.query[Condition](eligibilityCriteria.fhir_query)

    if (bundle.entry.isDefined) {
      bundle.entry.get map { bundleEntry =>
        if (bundleEntry.resource.isDefined) {
          patientIds += bundleEntry.resource.get.subject.reference.getOrElse(null)
        }
      }
    }

    // TODO fhir_path

    patientIds.toList
  }

  private def findEligiblePatientsInObservation(eligibilityCriteria: EligibilityCriteria): Seq[String] = {
    val fhirClient = FHIRClient()

    val patientIds = ListBuffer[String]()

    val bundle: Bundle[Observation] = fhirClient.query[Observation](eligibilityCriteria.fhir_query)

    if (bundle.entry.isDefined) {
      bundle.entry.get map { bundleEntry =>
        if (bundleEntry.resource.isDefined) {
          if (bundleEntry.resource.get.subject.isDefined) {
            patientIds += bundleEntry.resource.get.subject.get.reference.getOrElse(null)
          }
        }
      }
    }

    // TODO fhir_path

    patientIds.toList
  }
}
