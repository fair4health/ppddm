package ppddm.manager.controller.project

import com.typesafe.scalalogging.Logger
import ppddm.core.rest.model.Project
import ppddm.manager.Manager

import org.mongodb.scala.model.Filters._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

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
      .recoverWith {
        case e: Exception =>
          logger.error("Error while inserting a Project with project_id:{} into the database", projectWithId.project_id.get, e)
          Future.failed(e)
      }
  }

  def getProject(project_id: String): Future[Project] = {
    db.getCollection[Project](COLLECTION_NAME).find(equal("project_id", project_id)).first().toFuture()
  }

}
