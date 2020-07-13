package ppddm.manager.controller.project

import com.typesafe.scalalogging.Logger
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.{FindOneAndReplaceOptions, ReturnDocument}
import ppddm.core.exception.DBException
import ppddm.core.rest.model.Project
import ppddm.manager.Manager

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Controller object for the Projects. Each use-case of FAIR4Health can be modeled as a Project.
 */
object ProjectController {

  val COLLECTION_NAME: String = "project"

  private val logger: Logger = Logger(this.getClass)
  private val db = Manager.mongoDB.getDatabase

  /**
   * Creates a new Project on the Platform Repository
   *
   * @param project The project to be created
   * @return The created Project object with a unique project_id in it
   */
  def createProject(project: Project): Future[Project] = {
    val projectWithId = project.withUniqueProjectId // Create a new, timestamped, Project object with a unique identifier
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

  /**
   * Retrieves the Project from the Platform Repository.
   *
   * @param project_id The unique identifier of the Project
   * @return The Project if project_id is valid, None otherwise.
   */
  def getProject(project_id: String): Future[Option[Project]] = {
    db.getCollection[Project](COLLECTION_NAME).find(equal("project_id", project_id)).first().headOption()
  }

  /**
   * Retrieves all Projects from the Platform Repository.
   *
   * @return The list of all Projects in the Platform Repository, empty list if there are no Projects.
   */
  def getAllProjects: Future[Seq[Project]] = {
    db.getCollection[Project](COLLECTION_NAME).find().toFuture()
  }

  /**
   * Updates the Project by doing a replacement.
   *
   * @param project The Project object to be updated.
   * @return The updated Project object if operation is successful, None otherwise.
   */
  def updateProject(project: Project): Future[Option[Project]] = {
    // TODO: Add some integrity checks before document replacement
    db.getCollection[Project](COLLECTION_NAME).findOneAndReplace(
      equal("project_id", project.project_id.get),
      project,
      FindOneAndReplaceOptions().returnDocument(ReturnDocument.AFTER)).headOption()
  }

  /**
   * Deletes Project from the Platform Repository.
   *
   * @param project_id The unique identifier of the Project to be deleted.
   * @return The deleted Project object if operation is successful, None otherwise.
   */
  def deleteProject(project_id: String): Future[Option[Project]] = {
    db.getCollection[Project](COLLECTION_NAME).findOneAndDelete(equal("project_id", project_id)).headOption()
  }

}
