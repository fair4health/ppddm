package ppddm.manager.controller.project

import akka.Done
import com.typesafe.scalalogging.Logger
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.{FindOneAndReplaceOptions, ReturnDocument}
import ppddm.core.exception.DBException
import ppddm.core.rest.model.Project
import ppddm.manager.Manager
import ppddm.manager.controller.dataset.DatasetController
import ppddm.manager.controller.dm.DataMiningModelController
import ppddm.manager.controller.featureset.FeaturesetController
import ppddm.manager.controller.prospective.ProspectiveStudyController

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, TimeoutException}
import scala.concurrent.duration.Duration

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
    if (project.project_id.isDefined) {
      throw new IllegalArgumentException("If you want to create a new project, please provide it WITHOUT a project_id")
    }
    val projectWithId = project.withUniqueProjectId // Create a new, timestamped, Project object with a unique identifier
    db.getCollection[Project](COLLECTION_NAME).insertOne(projectWithId).toFuture() // insert into the database
      .map { result =>
        val _id = result.getInsertedId.asObjectId().getValue.toString
        logger.debug("Inserted document _id:{} and project_id:{}", _id, projectWithId.project_id.get)
        projectWithId
      }
      .recover {
        case e: Exception =>
          val msg = s"Error while inserting a Project with project_id:${projectWithId.project_id.get} into the database."
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
    db.getCollection[Project](COLLECTION_NAME).find(equal("project_id", project_id))
      .first()
      .headOption()
      .recover {
        case e: Exception =>
          val msg = s"Error while retrieving a Project with project_id:${project_id} from the database."
          throw DBException(msg, e)
      }
  }

  /**
   * Retrieves all Projects from the Platform Repository.
   *
   * @return The list of all Projects in the Platform Repository, empty list if there are no Projects.
   */
  def getAllProjects: Future[Seq[Project]] = {
    db.getCollection[Project](COLLECTION_NAME).find().toFuture()
      .recover {
        case e: Exception =>
          val msg = s"Error while retrieving all Projects from the database."
          throw DBException(msg, e)
      }
  }

  /**
   * Updates the Project by doing a replacement.
   *
   * @param project The Project object to be updated.
   * @return The updated Project object if operation is successful, None otherwise.
   */
  def updateProject(project: Project): Future[Option[Project]] = {
    db.getCollection[Project](COLLECTION_NAME).findOneAndReplace(
      equal("project_id", project.project_id.get),
      project,
      FindOneAndReplaceOptions().returnDocument(ReturnDocument.AFTER))
      .headOption()
      .recover {
        case e: Exception =>
          val msg = s"Error while updating the Project with project_id:${project.project_id.get} in the database."
          throw DBException(msg, e)
      }
  }

  /**
   * Deletes Project from the Platform Repository.
   *
   * @param project_id              The unique identifier of the Project to be deleted.
   * @param withAssociatedResources if true, delete all associated resources (Featureset, Dataset, DataMiningModel, ProspectiveStudy) of this project
   *                                from the platform database and their associated resources on the distributed agents
   * @return The deleted Project object if operation is successful, None otherwise.
   */
  def deleteProject(project_id: String, withAssociatedResources: Boolean = false, isTest: Boolean = false): Future[Option[Project]] = {
    val f = if (withAssociatedResources) {
      FeaturesetController.getAllFeaturesets(project_id) flatMap { featuresets => // Find all featuresets under this project
        FeaturesetController.deleteManyFeaturesets(featuresets.map(_.featureset_id.get)) flatMap { deletedFeaturesetCount => // Delete all Featuresets with a single function call
          deletedFeaturesetCount
          DatasetController.getAllDatasets(project_id, isTest) flatMap { datasets => // Find all datasets under this project
            datasets foreach { dataset => //Delete them one by one
              try { // Wait for dataset deletion to be completed because it includes Agent communication
                Await.result(DatasetController.deleteDataset(dataset.dataset_id.get, isTest), Duration(60, TimeUnit.SECONDS))
              } catch {
                case e: TimeoutException =>
                  val msg = s"Dataset deletion cannot be completed within 60 seconds. Dataset: ${dataset.dataset_id.get} Project: ${project_id}"
                  logger.error(msg, e)
                  throw DBException(msg, e)
              }
            }
            DataMiningModelController.getAllDataMiningModels(project_id) flatMap { dataMiningModels => // Find all DataMiningModels under this project
              dataMiningModels foreach { dataMiningModel =>
                try { // Wait for data mining model deletion to be completed because it includes Agent communication
                  Await.result(DataMiningModelController.deleteDataMiningModel(dataMiningModel.model_id.get, isTest), Duration(60, TimeUnit.SECONDS))
                  Await.result(DataMiningModelController.deleteDataMiningModel(dataMiningModel.model_id.get, isTest), Duration(60, TimeUnit.SECONDS))
                } catch {
                  case e: TimeoutException =>
                    val msg = s"DataMiningModel deletion cannot be completed within 60 seconds. DataMiningModel: ${dataMiningModel.model_id.get} Project: ${project_id}"
                    logger.error(msg, e)
                    throw DBException(msg, e)
                }
              }
              ProspectiveStudyController.getAllProspectiveStudies(project_id) flatMap { prospectiveStudies => // Find all ProspectiveStudies under this project
                // Delete all ProspectiveStudies with a single function call
                ProspectiveStudyController.deleteManyProspectiveStudies(prospectiveStudies.map(_.prospective_study_id.get)) map { _ => Done }
              }
            }
          }
        }
      }
    } else {
      Future.apply(Done)
    }

    f flatMap { _ =>
      db.getCollection[Project](COLLECTION_NAME).findOneAndDelete(equal("project_id", project_id))
        .headOption()
        .recover {
          case e: Exception =>
            val msg = s"Error while deleting the Project with project_id:${project_id} from the database."
            throw DBException(msg, e)
        }
    }
  }

}
