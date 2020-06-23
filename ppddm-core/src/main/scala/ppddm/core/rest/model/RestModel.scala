package ppddm.core.rest.model

import java.time.LocalDateTime
import java.util.UUID

import ppddm.core.rest.model.DataSourceStatus.DataSourceStatus
import ppddm.core.rest.model.DatasetStatus.DatasetStatus
import ppddm.core.rest.model.ProjectType.ProjectType
import ppddm.core.rest.model.VariableDataType.VariableDataType
import ppddm.core.rest.model.VariableType.VariableType

sealed class ModelClass

case class Project(project_id: Option[String],
                   name: String,
                   description: String,
                   project_type: ProjectType,
                   created_by: String, // The user ID who creates this Project
                   created_on: Option[LocalDateTime],
                   updated_on: Option[LocalDateTime]) extends ModelClass {

  def withUniqueProjectId: Project = {
    this.copy(project_id = Some(UUID.randomUUID().toString), created_on = Some(LocalDateTime.now()))
  }
}

case class Featureset(featureset_id: Option[String],
                      name: String,
                      description: String,
                      variables: Option[Seq[Variable]],
                      created_by: String,
                      created_on: Option[LocalDateTime],
                      updated_on: Option[LocalDateTime]) extends ModelClass {

  def withUniqueFeaturesetId: Featureset = {
    this.copy(featureset_id = Some(UUID.randomUUID().toString))
  }
}

case class Variable(name: String,
                    description: Option[String],
                    fhir_query: String,
                    fhir_path: String,
                    variable_data_type: VariableDataType,
                    variable_type: VariableType) extends ModelClass

final case class Dataset(dataset_id: Option[String],
                         featureset: Featureset,
                         name: String,
                         description: String,
                         status: Option[DatasetStatus],
                         eligibility_criteria: Option[Seq[EligibilityCriteria]],
                         data_sources: Option[Seq[DataSource]],
                         created_by: String,
                         created_on: Option[LocalDateTime],
                         updated_on: Option[LocalDateTime]) extends ModelClass {

  def withUniqueDatasetId: Dataset = {
    this.copy(dataset_id = Some(UUID.randomUUID().toString))
  }

  def withStatus(status: DatasetStatus): Dataset = {
    this.copy(status = Some(status))
  }

  def withDataSources(data_sources: Seq[DataSource]): Dataset = {
    this.copy(data_sources = Some(data_sources))
  }

}

final case class EligibilityCriteria(fhir_query: String,
                                     fhir_path: Option[String]) extends ModelClass

final case class DataSource(data_source_id: Option[String],
                            name: String,
                            status: DataSourceStatus,
                            endpoint: String,
                            number_of_records: Option[Long],
                            variable_statistics: Option[Seq[VariableStatistics]]) extends ModelClass

final case class VariableStatistics(variable: Variable,
                                    min_value: Option[Double],
                                    max_value: Option[Double],
                                    null_percentage: Option[Double]) extends ModelClass
