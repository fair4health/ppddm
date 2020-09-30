package ppddm.manager.controller.dm

import com.typesafe.scalalogging.Logger
import ppddm.core.exception.DBException
import ppddm.core.rest.model.DataMiningModel
import ppddm.manager.Manager

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Controller object for the DataMiningModels.
 *
 * A DataMiningModel is created within a Project by providing a Dataset and sequence of Algorithms to be executed
 * on the Dataset. Since the Dataset is distributed over the Agents (DatasetSources), execution of the algorithms
 * is performed with the help of DistributedDataMiningManager. Distributed algorithm execution should be orchestrated
 * for each Algorithm and the final model should be always updated in the associated DataMiningModel on Platform
 * Repository.
 */
object DataMiningModelController {

  val COLLECTION_NAME: String = "dataminingmodel"

  private val logger: Logger = Logger(this.getClass)
  private val db = Manager.mongoDB.getDatabase

  /**
   * Creates a new DataMiningModel on the Platform Repository
   * and invokes the agents to start their algorithm execution processes.
   *
   * @param dataMiningModel The DataMiningModel to be created
   * @return The created DataMiningModel with a unique model_id in it
   */
  def createDataMiningModel(dataMiningModel: DataMiningModel): Future[DataMiningModel] = {
    // Create a new DataMiningModel object with a unique identifier
    val dataMiningModelWithId = dataMiningModel.withUniqueModelId

    // Invoke agents to start data mining (algorithm execution) processes
    // Returns a new Dataset object with data sources and execution state "querying"
    DistributedDataMiningManager.invokeAgentsDataMining(dataMiningModelWithId) flatMap { dataMiningModelWithDataMiningSources =>
      logger.info("Algorithm execution (data mining) endpoints are invoked for the selected Agents (datasources) of the associated Dataset")
      db.getCollection[DataMiningModel](COLLECTION_NAME).insertOne(dataMiningModelWithDataMiningSources).toFuture() // insert into the database
        .map { result =>
          val _id = result.getInsertedId.asObjectId().getValue.toString
          logger.debug("Inserted document _id:{} and model_id:{}", _id, dataMiningModelWithDataMiningSources.model_id.get)
          dataMiningModelWithDataMiningSources
        }
        .recover {
          case e: Exception =>
            val msg = s"Error while inserting a DataMiningModel with model_id:${dataMiningModelWithDataMiningSources.model_id.get} into the database."
            throw DBException(msg, e)
        }
    }
  }

}
