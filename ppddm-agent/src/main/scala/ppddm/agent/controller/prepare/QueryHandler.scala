package ppddm.agent.controller.prepare

import ppddm.agent.config.AgentConfig
import ppddm.core.rest.model.EligibilityCriteria

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Handles the Queries on the HL7 FHIR Repository
 */
object QueryHandler {

  /**
   * Retrieves the eligible patients from the FHIR Repository
   *
   * @param eligibilityCriteria
   */
  def executeEligibilityQuery(eligibilityCriteria: Seq[EligibilityCriteria]): Future[Seq[String]] = {
    AgentConfig.fhirEndpoint
    Future {Seq.empty}
  }

}
