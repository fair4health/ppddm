package ppddm.manager.controller.prospective

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.mongodb.scala.model.Filters.{equal, in}
import org.mongodb.scala.model.{FindOneAndReplaceOptions, ReturnDocument}
import ppddm.core.ai.{PipelineModelEncoderDecoder, Predictor}
import ppddm.core.exception.DBException
import ppddm.core.rest.model.{DataType, PredictionRequest, PredictionResult, ProspectiveStudy, VariableType}
import ppddm.core.util.DataPreparationUtil
import ppddm.manager.Manager
import ppddm.manager.store.ManagerDataStoreManager

import java.time.LocalDateTime
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object ProspectiveStudyController {

  val COLLECTION_NAME: String = "prospectivestudy"

  private val logger: Logger = Logger(this.getClass)
  private val db = Manager.mongoDB.getDatabase

  implicit val sparkSession: SparkSession = Manager.dataMiningEngine.sparkSession

  /**
   * Creates a new ProspectiveStudy on the Platform Repository
   *
   * @param prospectiveStudy The ProspectiveStudy to be created
   * @return The created ProspectiveStudy object with a unique prospective_study_id in it
   */
  def createProspectiveStudy(prospectiveStudy: ProspectiveStudy): Future[ProspectiveStudy] = {
    if (prospectiveStudy.prospective_study_id.isDefined) {
      throw new IllegalArgumentException("If you want to create a new prospective study, please provide it WITHOUT a prospective_study_id")
    }
    if (prospectiveStudy.predictions.isEmpty) {
      throw new IllegalArgumentException(s"A prospective study must include at lease on prediction in it while saving.")
    }
    logger.debug("Creating prospective study...")
    val prospectiveStudyWithId = prospectiveStudy.withUniqueProspectiveStudyId // Create a new, timestamped, ProspectiveStudy object with a unique identifier
    db.getCollection[ProspectiveStudy](COLLECTION_NAME).insertOne(prospectiveStudyWithId).toFuture() // insert into the database
      .map { result =>
        val _id = result.getInsertedId.asObjectId().getValue.toString
        logger.debug("Inserted document _id:{} and prospective_study_id:{}", _id, prospectiveStudyWithId.prospective_study_id.get)
        prospectiveStudyWithId
      }
      .recover {
        case e: Exception =>
          val msg = s"Error while inserting a ProspectiveStudy with prospective_study_id:${prospectiveStudyWithId.prospective_study_id.get} into the database."
          throw DBException(msg, e)
      }
  }

  /**
   * Retrieves the ProspectiveStudy from the Platform Repository.
   *
   * @param prospective_study_id The unique identifier of the ProspectiveStudy
   * @return The ProspectiveStudy if prospective_study_id is valid, None otherwise.
   */
  def getProspectiveStudy(prospective_study_id: String): Future[Option[ProspectiveStudy]] = {
    logger.debug(s"Retrieving prospective study with id ${prospective_study_id}...")
    db.getCollection[ProspectiveStudy](COLLECTION_NAME).find(equal("prospective_study_id", prospective_study_id))
      .first()
      .headOption()
      .recover {
        case e: Exception =>
          val msg = s"Error while retrieving a ProspectiveStudy with prospective_study_id:${prospective_study_id} from the database."
          throw DBException(msg, e)
      }
  }

  /**
   * Retrieves all ProspectiveStudies of a Project from the Platform Repository.
   *
   * @param project_id The project ID whose ProspectiveStudies are to be retrieved.
   * @return The list of all ProspectiveStudies for the given project, empty list if there are no ProspectiveStudies.
   */
  def getAllProspectiveStudies(project_id: String): Future[Seq[ProspectiveStudy]] = {
    logger.debug(s"Retrieving all prospective studies under project: ${project_id} ...")
    db.getCollection[ProspectiveStudy](COLLECTION_NAME).find(equal("project_id", project_id)).toFuture()
      .recover {
        case e: Exception =>
          val msg = s"Error while retrieving all ProspectiveStudies from the database."
          throw DBException(msg, e)
      }
  }

  /**
   * Updates the ProspectiveStudy by doing a replacement.
   *
   * @param prospectiveStudy The ProspectiveStudy object to be updated.
   * @return The updated ProspectiveStudy object if operation is successful, None otherwise.
   */
  def updateProspectiveStudy(prospectiveStudy: ProspectiveStudy): Future[Option[ProspectiveStudy]] = {
    if (prospectiveStudy.predictions.isEmpty) {
      throw new IllegalArgumentException(s"A prospective study must include at lease on prediction in it while updating. ${prospectiveStudy.prospective_study_id.get}")
    }

    logger.debug(s"Updating prospective study with id ${prospectiveStudy.prospective_study_id}...")
    db.getCollection[ProspectiveStudy](COLLECTION_NAME).findOneAndReplace(
      equal("prospective_study_id", prospectiveStudy.prospective_study_id.get),
      prospectiveStudy.withLastUpdated,
      FindOneAndReplaceOptions().returnDocument(ReturnDocument.AFTER))
      .headOption()
      .recover {
        case e: Exception =>
          val msg = s"Error while updating the ProspectiveStudy with prospective_study_id:${prospectiveStudy.prospective_study_id.get} in the database."
          throw DBException(msg, e)
      }
  }

  /**
   * Deletes ProspectiveStudy from the Platform Repository.
   *
   * @param prospective_study_id The unique identifier of the ProspectiveStudy to be deleted.
   * @return The deleted ProspectiveStudy object if operation is successful, None otherwise.
   */
  def deleteProspectiveStudy(prospective_study_id: String): Future[Option[ProspectiveStudy]] = {
    logger.debug(s"Deleting prospective study with id ${prospective_study_id}...")
    db.getCollection[ProspectiveStudy](COLLECTION_NAME).findOneAndDelete(equal("prospective_study_id", prospective_study_id))
      .headOption()
      .recover {
        case e: Exception =>
          val msg = s"Error while deleting the ProspectiveStudy with prospective_study_id:${prospective_study_id} from the database."
          throw DBException(msg, e)
      }
  }

  /**
   * Deletes all ProspectiveStudies of the given prospective_study_ids from the Platform Repository.
   *
   * @param prospective_study_ids A sequence of unique identifiers of the ProspectiveStudies to be deleted
   * @return The number of deleted ProspectiveStudies objects if the operation is successful, None otherwise
   */
  def deleteManyProspectiveStudies(prospective_study_ids: Seq[String]): Future[Option[Long]] = {
    // (prospective_study_ids:_*) to "unroll" the collection for passing its individual elements as varargs
    db.getCollection[ProspectiveStudy](COLLECTION_NAME).deleteMany(in("prospective_study_id", prospective_study_ids:_*))
      .headOption()
      .map { deleteResult =>
        deleteResult.map(_.getDeletedCount)
      }
      .recover {
        case e: Exception =>
          val msg = s"Error while deleting the ProspectiveStudies with prospective_study_ids:${prospective_study_ids} from the database."
          throw DBException(msg, e)
      }
  }

  /**
   * Makes the prediction with the BoostedModel for the given patient
   *
   * @param predictionRequest The PredictionRequest containing the patient data and data mining model id
   * @return the PredictionResult containing the prediction value
   */
  def predict(predictionRequest: PredictionRequest): Future[PredictionResult] = {
    Future {
      logger.debug("Prediction request received.")

      logger.debug("Creating data frame...")
      // For each variable in feature set, find the value from predictionRequest
      val variableValues = predictionRequest.data_mining_model.dataset.featureset.variables
        .filter(variable => variable.variable_type == VariableType.INDEPENDENT) // do not include dependent variable as it is not provided in PredictionRequest
        .map { variable =>
          val variableParameter = predictionRequest.variables.find(v => v.name == variable.name)
          if (variableParameter.isDefined) {
            if (variableParameter.get.data_type == DataType.INTEGER || variableParameter.get.data_type == DataType.DOUBLE)
              variableParameter.get.value.toDouble
            else variableParameter.get.value
          }
        }

      // First column must be pid, last column must be the dependent variable
      // These values are not provided in the variables in prediction request, hence provide manually in here
      // The last one is just for satisfying the schema in PipelineModel, hence -99.9 is just an arbitrary value
      val rowSeq = Seq(Row.fromSeq(Seq(predictionRequest.identifier) ++ variableValues ++ Seq(-99.9)))

      val structureSchema = DataPreparationUtil.generateSchema(predictionRequest.data_mining_model.dataset.featureset)

      val dataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rowSeq), structureSchema)

      logger.debug("Data frame is ready:")
      dataFrame.printSchema()
      dataFrame.show(false)

      logger.debug("Predicting with weak models...")
      val testPredictionTuples = predictionRequest.data_mining_model.getSelectedBoostedModel().weak_models.map { weakModel =>
        val pipelineModel = PipelineModelEncoderDecoder.fromString(weakModel.fitted_model.get, ManagerDataStoreManager.getTmpPath())
        (weakModel.weight.get, pipelineModel.transform(dataFrame))
      }

      logger.debug("Predicting with boosted model...")
      val testPredictionDF = Predictor.predictWithWeightedAverageOfPredictions(testPredictionTuples)
      val pArray = testPredictionDF.select("prediction", "positiveProbability").rdd.map(r => (r.getDouble(0), r.getDouble(1))).collect()

      logger.debug(s"PredictionResult is ready for patient id: ${predictionRequest.identifier} and data_mining_model_id: ${predictionRequest.data_mining_model.model_id}")
      PredictionResult(predictionRequest.identifier, predictionRequest.variables, pArray.head._1, pArray.head._2, LocalDateTime.now())
    }
  }
}
