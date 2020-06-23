package ppddm.manager.controller.featureset

import com.typesafe.scalalogging.Logger
import ppddm.manager.Manager
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates.{combine, set}
import ppddm.core.exception.{DBException, NotFoundException}
import ppddm.core.rest.model.Featureset

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
   * Saves the feature set object to the Platform Repository.
   * @param featureset The feature set object containing name, description and list of variables.
   * @return The feature set object with a unique identifier if operation is successful, error otherwise.
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
   * Retrieves all the feature sets from the Platform Repository.
   * @return empty list if there is no feature set in the repository, the list of featuresets otherwise.
   */
  def getAllFeaturesets(): Future[Seq[Featureset]] = {
    db.getCollection[Featureset](COLLECTION_NAME).find().toFuture()
  }

  /**
   * Retrieves the feature set with specific unique identifier from the Platform Repository.
   * @param featureset_id The unique identifier of the feature set to be returned
   * @return null if featureset_id is not valid, the Featureset object otherwise.
   */
  def getFeatureset(featureset_id: String): Future[Option[Featureset]] = {
    db.getCollection[Featureset](COLLECTION_NAME).find(equal("featureset_id", featureset_id)).first().headOption()
  }

  /**
   * Updates the feature set.
   * @param featureset The Featureset object to be updated.
   * @return the updated Featureset object if operation is successful, error otherwise.
   */
  def updateFeatureset(featureset: Featureset): Future[Featureset] = { // TODO to be updated
    db.getCollection[Featureset](COLLECTION_NAME).findOneAndUpdate(
      equal("featureset_id", featureset.featureset_id.get),
      combine(set("name", featureset.name), set("description", featureset.description))
    ).toFuture()
  }

  /**
   * Deletes featureset from the Platform Repository.
   * @param featureset_id The unique identifier of the featureset to be deleted.
   * @return the number of deleted records if operation is successful, error otherwise.
   */
  def deleteFeatureset(featureset_id: String): Future[Long] = {
    db.getCollection[Featureset](COLLECTION_NAME).deleteOne(equal("featureset_id", featureset_id)).toFuture() map { result =>
      val count = result.getDeletedCount
      if (count == 0) {
        throw NotFoundException(s"The Featureset with id:${featureset_id} not found")
      }
      count
    }
  }
}
