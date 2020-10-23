package ppddm.manager.controller.featureset

import com.typesafe.scalalogging.Logger
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.{FindOneAndReplaceOptions, ReturnDocument}
import ppddm.core.exception.DBException
import ppddm.core.rest.model.Featureset
import ppddm.manager.Manager

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Controller object for the Featuresets. Featureset is an data extraction specification from FHIR Repository.
 */
object FeaturesetController {

  val COLLECTION_NAME: String = "featureset"

  private val logger: Logger = Logger(this.getClass)
  private val db = Manager.mongoDB.getDatabase

  /**
   * Creates a new Featureset on the Platform Repository.
   *
   * @param featureset The Featureset to be created
   * @return The created Featureset with a unique featureset_id in it
   */
  def createFeatureset(featureset: Featureset): Future[Featureset] = {
    val featuresetWithId = featureset.withUniqueFeaturesetId // Create a new Featureset object with a unique identifier
    db.getCollection[Featureset](COLLECTION_NAME).insertOne(featuresetWithId).toFuture() // insert into the database
      .map { result =>
        val _id = result.getInsertedId.asObjectId().getValue.toString
        logger.debug("Inserted document _id:{} and featureset_id:{}", _id, featuresetWithId.featureset_id.get)
        featuresetWithId
      }
      .recover {
        case e: Exception =>
          val msg = s"Error while inserting a Featureset with featureset_id:${featuresetWithId.featureset_id.get} into the database."
          throw DBException(msg, e)
      }
  }

  /**
   * Retrieves the Featureset from the Platform Repository.
   *
   * @param featureset_id The unique identifier of the Featureset
   * @return The Featureset if featureset_id is valid, None otherwise.
   */
  def getFeatureset(featureset_id: String): Future[Option[Featureset]] = {
    db.getCollection[Featureset](COLLECTION_NAME).find(equal("featureset_id", featureset_id))
      .first()
      .headOption()
      .recover {
        case e: Exception =>
          val msg = s"Error while retrieving a Featureset with featureset_id:${featureset_id} from the database."
          throw DBException(msg, e)
      }
  }

  /**
   * Retrieves all Featuresets of a project from the Platform Repository.
   *
   * @param project_id The project ID whose Featuresets are to be retrieved.
   * @return The list of all Featuresets for the given project, empty list if there are no Featuresets.
   */
  def getAllFeaturesets(project_id: String): Future[Seq[Featureset]] = {
    db.getCollection[Featureset](COLLECTION_NAME).find(equal("project_id", project_id)).toFuture()
      .recover {
        case e: Exception =>
          val msg = s"Error while retrieving the Featuresets of the Project with project_id:${project_id} from the database."
          throw DBException(msg, e)
      }
  }

  /**
   * Updates the Featureset by doing a replacement.
   *
   * @param featureset The Featureset object to be updated.
   * @return The updated Featureset object if operation is successful, None otherwise.
   */
  def updateFeatureset(featureset: Featureset): Future[Option[Featureset]] = {
    // TODO: Add some integrity checks before document replacement
    db.getCollection[Featureset](COLLECTION_NAME).findOneAndReplace(
      equal("featureset_id", featureset.featureset_id.get),
      featureset,
      FindOneAndReplaceOptions().returnDocument(ReturnDocument.AFTER))
      .headOption()
      .recover {
        case e: Exception =>
          val msg = s"Error while updating the Featureset with featureset_id:${featureset.featureset_id.get} in the database."
          throw DBException(msg, e)
      }
  }

  /**
   * Deletes Featureset from the Platform Repository.
   *
   * @param featureset_id The unique identifier of the Featureset to be deleted.
   * @return The deleted Featureset object if operation is successful, None otherwise.
   */
  def deleteFeatureset(featureset_id: String): Future[Option[Featureset]] = {
    db.getCollection[Featureset](COLLECTION_NAME).findOneAndDelete(equal("featureset_id", featureset_id))
      .headOption()
      .recover {
        case e: Exception =>
          val msg = s"Error while deleting the Featureset with featureset_id:${featureset_id} from the database."
          throw DBException(msg, e)
      }
  }
}
