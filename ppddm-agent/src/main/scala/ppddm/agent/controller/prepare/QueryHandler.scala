package ppddm.agent.controller.prepare

import ppddm.core.fhir.{FHIRQuery, FHIRQueryWithQueryString}
import ppddm.core.rest.model.{EligibilityCriterion, EncounterBasedItem}

/**
 * Handles the Queries on the HL7 FHIR Repository
 */
object QueryHandler {

  def getPatientQuery(eligibilityCriteria: Seq[EligibilityCriterion]): FHIRQuery = {
    val patientEC = eligibilityCriteria.find(_.fhir_query.startsWith("/Patient"))
    val patientQuery = if (patientEC.isDefined) {
      FHIRQueryWithQueryString(patientEC.get.fhir_query, patientEC.get.fhir_path)
    } else {
      FHIRQueryWithQueryString("/Patient")
    }
    patientQuery
  }

  def getResourcesOfPatientsQuery(patientURIs: Set[String], fhir_query: String, fhir_path: Option[String]): FHIRQuery = {
    val queryString = if (fhir_query.contains("?")) fhir_query + '&' else fhir_query + '?'
    if (fhir_query.startsWith("/Patient")) {
      FHIRQueryWithQueryString(s"${queryString}_id=${patientURIs.map(_.substring(8)).mkString(",")}", fhir_path)
    } else {
      FHIRQueryWithQueryString(s"${queryString}subject=${patientURIs.mkString(",")}", fhir_path)
    }
  }

  def getResourcesOfEncountersQuery(encounterURIs: Set[String], encounterMap: Map[String, EncounterBasedItem],
                                    fhir_query: String, fhir_path: Option[String]): FHIRQuery = {
    val queryString = if (fhir_query.contains("?")) fhir_query + '&' else fhir_query + '?'
    if (fhir_query.startsWith("/Patient")) {
      // If the queried resource type is Patient, search by its id. Collect ids of subjects.
      FHIRQueryWithQueryString(s"${queryString}_id=${encounterMap.values.map(_.subject.substring(8)).mkString(",")}", fhir_path)
    } else if (fhir_query.startsWith("/Encounter")) {
      // If the queried resource type is Encounter, search by its id. Collect ids of encounters.
      FHIRQueryWithQueryString(s"${queryString}_id=${encounterURIs.map(_.substring(10)).mkString(",")}", fhir_path)
    } else if (fhir_path.isDefined && fhir_path.get.contains(":exists")) {
      // If the fhir_path expression is an "existence" expression, search by subject
      FHIRQueryWithQueryString(s"${queryString}subject=${encounterMap.values.map(_.subject).mkString(",")}", fhir_path)
    } else {
      // If the fhir_path expression is a "get value" expression, search by encounter
      if (fhir_query.startsWith("/MedicationStatement")) {
        FHIRQueryWithQueryString(s"${queryString}context=${encounterMap.keySet.mkString(",")}", fhir_path)
      } else {
        FHIRQueryWithQueryString(s"${queryString}encounter=${encounterMap.keySet.mkString(",")}", fhir_path)
      }
    }
  }

}
