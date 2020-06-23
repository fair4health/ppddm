package ppddm.core.rest.model

import java.time.LocalDateTime
import java.util.UUID

import ppddm.core.rest.model.ProjectType.ProjectType

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

case class Variable(name: String) extends ModelClass
