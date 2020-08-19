package ppddm.agent.controller.prepare

import ppddm.core.fhir.{FHIRQuery, FHIRQueryWithQueryString}
import ppddm.core.rest.model.EligibilityCriterion

/**
 * Handles the Queries on the HL7 FHIR Repository
 */
object QueryHandler {

  def getPatientQuery(eligibilityCriteria: Seq[EligibilityCriterion]): FHIRQuery = {
    // TODO: What if there are multiple Patient queries in the eligibilityCriteria
    val patientEC = eligibilityCriteria.find(_.fhir_query.startsWith("/Patient"))
    val patientQuery = if (patientEC.isDefined) {
      FHIRQueryWithQueryString(patientEC.get.fhir_query)
    } else {
      FHIRQueryWithQueryString("/Patient")
    }
    patientQuery
  }

  def getResoucesOfPatientsQuery(patientURIs: Set[String], eligibilityCriteria: EligibilityCriterion): FHIRQuery = {
    val queryString = if (eligibilityCriteria.fhir_query.contains("?"))
      eligibilityCriteria.fhir_query + '&'
    else eligibilityCriteria.fhir_query + '?'
    FHIRQueryWithQueryString(s"${queryString}subject=${patientURIs.mkString(",")}")
  }

}
