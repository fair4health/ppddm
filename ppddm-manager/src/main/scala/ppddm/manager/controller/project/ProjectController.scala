package ppddm.manager.controller.project

import com.typesafe.scalalogging.Logger
import ppddm.core.rest.model.Project
import ppddm.manager.Manager
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.Updates.{combine, set}
import ppddm.core.exception.{DBException, NotFoundException}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Controller object for the Projects. Each use-case of FAIR4Health can be modeled as a Project.
 */
object ProjectController {

  val COLLECTION_NAME: String = "project"

  private val logger: Logger = Logger(this.getClass)
  private val db = Manager.mongoDB.getDatabase

  def createProject(project: Project): Future[Project] = {
    val projectWithId = project.withUniqueProjectId // Create a new Project object with a unique identifier
    db.getCollection[Project](COLLECTION_NAME).insertOne(projectWithId).toFuture() // insert into the database
      .map { result =>
        val _id = result.getInsertedId.asObjectId().getValue.toString
        logger.debug("Inserted document _id:{} and projectId:{}", _id, project.project_id.get)
        projectWithId
      }
      .recover {
        case e: Exception =>
          val msg = s"Error while inserting a Project with project_id:${projectWithId.project_id.get} into the database."
          logger.error(msg, projectWithId.project_id.get, e)
          throw DBException(msg, e)
      }
  }

  def getProject(project_id: String): Future[Option[Project]] = {
    db.getCollection[Project](COLLECTION_NAME).find(equal("project_id", project_id)).first().headOption()
  }

  def getAllProjects: Future[Seq[Project]] = {
    db.getCollection[Project](COLLECTION_NAME).find().toFuture()
  }

  def updateProject(project: Project): Future[Project] = {
    db.getCollection[Project](COLLECTION_NAME).findOneAndUpdate(
      equal("project_id", project.project_id.get),
      combine(set("name", project.name), set("description", project.description))
    ).toFuture()
  }

  def deleteProject(project_id: String): Future[Long] = {
    db.getCollection[Project](COLLECTION_NAME).deleteOne(equal("project_id", project_id)).toFuture() map { result =>
      val count = result.getDeletedCount
      if (count == 0) {
        throw NotFoundException(s"The Project with id:${project_id} not found")
      }
      count
    }
  }

}
