package ppddm.core.rest.model

import java.time.LocalDateTime
import java.util.UUID

import ppddm.core.rest.model.DataSourceSelectionStatus.DataSourceSelectionStatus
import ppddm.core.rest.model.ExecutionState.ExecutionState
import ppddm.core.rest.model.ProjectType.ProjectType
import ppddm.core.rest.model.VariableDataType.VariableDataType
import ppddm.core.rest.model.VariableType.VariableType
import ppddm.core.util.URLUtil

sealed class ModelClass

case class Project(project_id: Option[String],
                   name: String,
                   description: String,
                   project_type: ProjectType,
                   created_by: String, // The user ID who creates this Project
                   created_on: Option[LocalDateTime]) extends ModelClass {

  def withUniqueProjectId: Project = {
    this.copy(project_id = Some(UUID.randomUUID().toString), created_on = Some(LocalDateTime.now()))
  }
}

case class Featureset(featureset_id: Option[String],
                      project_id: String,
                      name: String,
                      description: String,
                      variables: Option[Seq[Variable]],
                      created_by: String,
                      created_on: Option[LocalDateTime]) extends ModelClass {

  def withUniqueFeaturesetId: Featureset = {
    this.copy(featureset_id = Some(UUID.randomUUID().toString), created_on = Some(LocalDateTime.now()))
  }
}

case class Variable(name: String,
                    description: Option[String],
                    fhir_query: String,
                    fhir_path: String,
                    variable_data_type: VariableDataType,
                    variable_type: Option[VariableType]) extends ModelClass

final case class Dataset(dataset_id: Option[String],
                         project_id: String,
                         featureset: Featureset,
                         name: String,
                         description: String,
                         eligibility_criteria: Seq[EligibilityCriterion],
                         dataset_sources: Option[Seq[DatasetSource]],
                         execution_state: Option[ExecutionState],
                         created_by: String,
                         created_on: Option[LocalDateTime]) extends ModelClass {

  def withUniqueDatasetId: Dataset = {
    this.copy(dataset_id = Some(UUID.randomUUID().toString), created_on = Some(LocalDateTime.now()))
  }

  def withDataSources(dataset_sources: Seq[DatasetSource]): Dataset = {
    // Find the ExecutionState for the newly created Dataset
    val areAllAgentsFinished = dataset_sources
      // Set it to True if the execution_state is defined and it is recieved as FINAL from the Agent, False otherwise
      .map(s => s.execution_state.isDefined && s.execution_state.get == ExecutionState.FINAL)
      .reduceLeft((a, b) => a && b) // Logically AND the states. If all sources are True, then Dataset's states can become IN_PROGRESS
    val newExecutionState = if (areAllAgentsFinished) Some(ExecutionState.IN_PROGRESS) else execution_state
    this.copy(dataset_sources = Some(dataset_sources), execution_state = newExecutionState)
  }

  def withExecutionState(execution_state: ExecutionState): Dataset = {
    this.copy(execution_state = Some(execution_state))
  }

}

final case class EligibilityCriterion(fhir_query: String,
                                      fhir_path: Option[String]) extends ModelClass

final case class DatasetSource(data_source: DataSource,
                               data_source_statistics: Option[DataSourceStatistics],
                               selection_status: Option[DataSourceSelectionStatus],
                               execution_state: Option[ExecutionState]) extends ModelClass

final case class DataSource(datasource_id: String,
                            name: String,
                            endpoint: String) extends ModelClass {

  def getDataPreparationURI(dataset_id: Option[String] = None) = {
    if (dataset_id.isDefined) {
      URLUtil.append(endpoint, "prepare", dataset_id.get)
    } else {
      URLUtil.append(endpoint, "prepare")
    }
  }
}

final case class DataSourceStatistics(number_of_records: Long,
                                      variable_statistics: Seq[VariableStatistics]) extends ModelClass

final case class VariableStatistics(variable: Variable,
                                    min_value: Option[Double],
                                    max_value: Option[Double],
                                    null_percentage: Option[Double]) extends ModelClass

final case class DataPreparationRequest(dataset_id: String,
                                        data_source: DataSource,
                                        featureset: Featureset,
                                        eligibility_criteria: Seq[EligibilityCriterion],
                                        submitted_by: String) extends ModelClass

final case class DataPreparationResult(dataset_id: String,
                                       data_source: DataSource,
                                       datasource_statistics: DataSourceStatistics) extends ModelClass
