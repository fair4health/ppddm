package ppddm.core.rest.model

import java.time.LocalDateTime
import java.util.UUID

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
                      variables: Option[List[Variable]],
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
