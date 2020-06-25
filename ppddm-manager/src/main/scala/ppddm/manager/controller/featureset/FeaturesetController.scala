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
        logger.debug("Inserted document _id:{} and featuresetId:{}", _id, featuresetWithId.featureset_id.get)
        featuresetWithId
      }
      .recoverWith {
        case e: Exception =>
          val msg = s"Error while inserting a Featureset with featureset_id:${featuresetWithId.featureset_id.get} into the database."
          logger.error(msg, featuresetWithId.featureset_id.get, e)
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
    db.getCollection[Featureset](COLLECTION_NAME).find(equal("featureset_id", featureset_id)).first().headOption()
  }

  /**
   * Retrieves all Featuresets from the Platform Repository.
   *
   * @return The list of all Featuresets in the Platform Repository, empty list if there are no Featuresets.
   */
  def getAllFeaturesets: Future[Seq[Featureset]] = {
    db.getCollection[Featureset](COLLECTION_NAME).find().toFuture()
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
      FindOneAndReplaceOptions().returnDocument(ReturnDocument.AFTER)).headOption()
  }

  /**
   * Deletes Featureset from the Platform Repository.
   *
   * @param featureset_id The unique identifier of the Featureset to be deleted.
   * @return The deleted Featureset object if operation is successful, None otherwise.
   */
  def deleteFeatureset(featureset_id: String): Future[Option[Featureset]] = {
    db.getCollection[Featureset](COLLECTION_NAME).findOneAndDelete(equal("featureset_id", featureset_id)).headOption()
  }
}
