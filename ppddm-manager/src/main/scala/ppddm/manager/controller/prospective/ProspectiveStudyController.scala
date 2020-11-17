package ppddm.manager.controller.prospective

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.{FindOneAndReplaceOptions, ReturnDocument}
import ppddm.core.ai.{PipelineModelEncoderDecoder, Predictor}
import ppddm.core.exception.DBException
import ppddm.core.rest.model.{DataType, PredictionRequest, PredictionResult, ProspectiveStudy, VariableType}
import ppddm.core.util.DataPreparationUtil
import ppddm.manager.Manager
import ppddm.manager.controller.dm.DataMiningModelController
import ppddm.manager.exception.DataIntegrityException
import ppddm.manager.store.ManagerDataStoreManager

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
   * Retrieves all ProspectiveStudies from the Platform Repository.
   *
   * @return The list of all ProspectiveStudies in the Platform Repository, empty list if there are no ProspectiveStudies.
   */
  def getAllProspectiveStudies: Future[Seq[ProspectiveStudy]] = {
    logger.debug(s"Retrieving all prospective studies...")
    db.getCollection[ProspectiveStudy](COLLECTION_NAME).find().toFuture()
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
    logger.debug(s"Updating prospective study with id ${prospectiveStudy.prospective_study_id}...")
    // TODO: Add some integrity checks before document replacement
    db.getCollection[ProspectiveStudy](COLLECTION_NAME).findOneAndReplace(
      equal("prospective_study_id", prospectiveStudy.prospective_study_id.get),
      prospectiveStudy,
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
   * Makes the prediction with the BoostedModel for the given patient
   * @param predictionRequest The PredictionRequest containing the patient data and data mining model id
   * @return the PredictionResult containing the prediction value
   */
  def predict(predictionRequest: PredictionRequest): Future[PredictionResult] = {
    logger.debug("Prediction request received.")

    DataMiningModelController.getDataMiningModel(predictionRequest.data_mining_model_id) map { dataMiningModelOption =>
      if (dataMiningModelOption.isEmpty) {
        throw DataIntegrityException(s"ProspectiveStudyController cannot access DataMiningModel with model_id:${predictionRequest.data_mining_model_id}. This should not have happened!!")
      }
      val dataMiningModel = dataMiningModelOption.get

      logger.debug("Creating data frame...")
      // For each variable in feature set, find the value from predictionRequest
      val variableValues = dataMiningModel.dataset.featureset.variables.get
          .filter(variable => variable.variable_type == VariableType.INDEPENDENT) // do not include dependent variable as it is not provided in PredictionRequest
          .map { variable =>
            val variableParameter = predictionRequest.variables.find( v => v.name == variable.name)
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

      val structureSchema = DataPreparationUtil.generateSchema(dataMiningModel.dataset.featureset)

      val dataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rowSeq), structureSchema)

      logger.debug("Data frame is ready:")
      dataFrame.printSchema()
      dataFrame.show(false)

      logger.debug("Predicting with weak models...")
      val testPredictionTuples = dataMiningModel.getSelectedBoostedModel().weak_models.map { weakModel =>
        val pipelineModel = PipelineModelEncoderDecoder.fromString(weakModel.fitted_model, ManagerDataStoreManager.getTmpPath())
        (weakModel.weight.get, pipelineModel.transform(dataFrame))
      }

      logger.debug("Predicting with boosted model...")
      val testPredictionDF = Predictor.predictWithWeightedAverageOfPredictions(testPredictionTuples)
      val predictionArray = testPredictionDF.select("prediction").rdd.map(r => (r.getDouble(0))).collect()

      logger.debug(s"PredictionResult is ready for patient id: ${predictionRequest.identifier} and data_mining_model_id: ${predictionRequest.data_mining_model_id}")
      PredictionResult(predictionRequest.data_mining_model_id, predictionRequest.identifier, predictionRequest.variables, predictionArray.head)
    }
  }
}
