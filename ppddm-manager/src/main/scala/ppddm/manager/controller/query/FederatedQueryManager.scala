package ppddm.manager.controller.query

import akka.Done
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Accept, Authorization}
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.typesafe.scalalogging.Logger
import ppddm.core.rest.model._
import ppddm.manager.exception.AgentCommunicationException
import ppddm.manager.registry.AgentRegistry

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/* Import the ActorSystem */
import ppddm.manager.config.ManagerExecutionContext._

/**
 * This object handles the interaction with the Agents/Datasources so that the fedarated data preparation queries can be
 * executed on each Datasource endpoint.
 */
object FederatedQueryManager {

  private val logger: Logger = Logger(this.getClass)

  // Default headers
  private val defaultHeaders = List(Accept(MediaTypes.`application/json`))

  // TODO: How to obtain the access token? Discuss the authentication & authorization with ATOS.
  private val accessToken: String = "token123"

  /**
   * This function retrieves the registered agents (datasources) from the AgentRegistry and then invokes the
   * data preparation method of each of them in parallel for the given dataset.
   *
   * @param dataset The Dataset for which the data preparation endpoint of the agents will be invoked
   * @return A new Dataset populated with the invoked DatasetSource objects.
   */
  def invokeAgentsDataPreparation(dataset: Dataset): Future[Dataset] = {
    logger.debug("I will invoke the prepare endpoints of the registered agents")

    Future.sequence(
      AgentRegistry.dataSources.map { dataSource =>
        invokeDataPreparation(dataSource, dataset)
      }
    ) map { responses =>
      val failedAgents = responses.collect { case Failure(x) => x }
      if (failedAgents.nonEmpty) {
        logger.error("There are {} agents (data sources) out of {} which returned error on data preparation request.", failedAgents.size, responses.size)
        failedAgents.foreach(logger.error("Error during Agent communication for data preparation", _))
      }

      val successfulAgents = responses.collect { case Success(x) => x }
      dataset
        .withDataSources(successfulAgents) // create a new Dataset with the DatasetSources which are QUERYING
        .withExecutionState(ExecutionState.QUERYING) // Set the ExecutionState of the Dataset itself to QUERYING
    }
  }

  /**
   * Invokes the data preparation endpoint of the given DataSource for the given Dataset
   *
   * @param dataSource
   * @param dataset
   * @return
   */
  private def invokeDataPreparation(dataSource: DataSource, dataset: Dataset): Future[Try[DatasetSource]] = {
    val dataPreparationRequest: DataPreparationRequest = DataPreparationRequest(dataset.dataset_id.get, dataSource, dataset.featureset, dataset.eligibility_criteria, dataset.created_by)
    val uri = Uri(dataSource.getDataPreparationURI())

    /* To use the toJson, toPrettyJson methods of the JsonFormatter */
    import ppddm.core.util.JsonFormatter._

    val request = HttpRequest(
      uri = uri,
      method = HttpMethods.POST,
      headers = defaultHeaders)
      .withHeaders(Authorization(headers.OAuth2BearerToken(accessToken)))
      .withEntity(ContentTypes.`application/json`, dataPreparationRequest.toJson)

    logger.debug("Invoking agent data preparation on URI:{} for dataset_id: {} & dataset_name: {}", uri, dataset.dataset_id.get, dataset.name)

    Http().singleRequest(request).map(Try(_)) flatMap {
      case Success(res) =>
        res.status match {
          case StatusCodes.OK =>
            logger.debug("Agent data preparation invocation successful on URI:{} for dataset_id: {} & dataset_name: {}", uri, dataset.dataset_id.get, dataset.name)
            Future {
              Success(DatasetSource(dataSource, None, None, Some(ExecutionState.QUERYING)))
            }
          case _ =>
            // I got status code I didn't expect so I wrap it along with body into Future failure
            Unmarshal(res.entity).to[String].flatMap { body =>
              throw AgentCommunicationException(dataSource.name, dataSource.endpoint, s"The response status is ${res.status} [${request.uri}] and response body is $body")
            }
        }
      case Failure(e) =>
        throw e
    } recover {
      case e: Exception =>
        Failure(AgentCommunicationException(dataSource.name, dataSource.endpoint, "Exception while connecting to the Agent for data preparation", e))
    }
  }

  /**
   * Asks the data preparation results of the dataset to the DatasetSources defined within the given dataset.
   *
   * @param dataset
   * @return A new Dataset which contains the new DatasetSources based on the retrieved results
   */
  def askAgentsDataPreparationResults(dataset: Dataset): Future[Dataset] = {
    if (dataset.dataset_sources.isEmpty) {
      val msg = s"You want me to ask the data preparation results of this dataset with id:${dataset.dataset_id} and " +
        s"name:${dataset.name} HOWEVER there are no DatasetSources for this Dataset"
      logger.error(msg)
      throw new InternalError(msg)
    }

    logger.debug("I will ask for the DataPreparationResults from {} DatasetSources of the Dataset with id:{} and name:{}",
      dataset.dataset_sources.get.size, dataset.dataset_id, dataset.name)

    Future.sequence(
      dataset.dataset_sources.get.map { datasetSource: DatasetSource => // For each datasetSource in this set (actually, for each DataSource)
        getPreparedDataStatistics(datasetSource.data_source, dataset) // Ask for the data preparation results (do this in parallel)
      }
    ) map { responses: Seq[Option[DataPreparationResult]] => // Join the responses coming from different data sources (Agents)
      logger.debug("DataPreparationResults have been retrieved from all {} data sources (Agents) of the dataset.", responses.size)
      responses.map(result => { // For each DataPreparationResult
        result map { dataPreparationResult => // Create a corresponding DatasetSource object
          DatasetSource(dataPreparationResult.data_source, Some(dataPreparationResult.datasource_statistics), None, Some(ExecutionState.FINAL))
        }
      })
        .filter(_.isDefined) // Keep the the data sources which produced the results
        .map(_.get) // Get rid of the Option since we eliminated the None elements above
    } map { datasetSourcesWithResult: Seq[DatasetSource] => // DatasetSources which finished data preparation
      val updatedDatasetSources = dataset.dataset_sources.get map { existingDatasetSource => // Iterate over the existing DatasetSources of the dataset
        // decide whether there is a data preparation result for the existingDatasetSource
        val finishedDatasetSource = datasetSourcesWithResult.find(_.data_source.datasource_id == existingDatasetSource.data_source.datasource_id)
        // Return the DatasetSource if that has a result, otherwise keep the existing DatasetSource in the list
        if(finishedDatasetSource.isDefined) finishedDatasetSource.get else existingDatasetSource
      }
      dataset.withDataSources(updatedDatasetSources) // create a new Dataset with the updatedDatasetSources
    }
  }

  /**
   * Asks the DataPreparationResult from the given dataSource for the given dataset.
   *
   * @param dataSource
   * @param dataset
   * @return An Option[DataPreparationResult]. If the result is None, that means the data has not been prepared yet.
   */
  def getPreparedDataStatistics(dataSource: DataSource, dataset: Dataset): Future[Option[DataPreparationResult]] = {
    val uri = Uri(dataSource.getDataPreparationURI(dataset.dataset_id))
    val request = HttpRequest(
      uri = uri,
      method = HttpMethods.GET,
      headers = defaultHeaders)
      .withHeaders(Authorization(headers.OAuth2BearerToken(accessToken)))

    logger.debug("Asking the data preparation result to the Agent on URI:{} for dataset_id: {} & dataset_name: {}", uri, dataset.dataset_id.get, dataset.name)

    /* So that we can Unmarshal to DataPreparationResult */
    import ppddm.core.rest.model.Json4sSupport._

    Http().singleRequest(request).map(Try(_)) flatMap {
      case Success(res) =>
        res.status match {
          case StatusCodes.OK =>
            Unmarshal(res.entity).to[DataPreparationResult] map { Some (_) }
          case StatusCodes.NotFound =>
            Future.apply(Option.empty[DataPreparationResult])
          case _ =>
            Unmarshal(res.entity).to[String].map { body =>
              throw AgentCommunicationException(dataSource.name, dataSource.endpoint, s"The response status is ${res.status} [${request.uri}] and response body is $body")
            }
        }
      case Failure(e) =>
        throw e
    } recover {
      case e: Exception =>
        throw AgentCommunicationException(dataSource.name, dataSource.endpoint, "Exception while connecting to the Agent for asking the data preparation result", e)
    }

  }

  /**
   * Deletes the extracted Dataset and Statistics from the Agents.
   *
   * @param dataSource
   * @param dataset
   * @return
   */
  def deleteDatasetAndStatistics(dataSource: DataSource, dataset: Dataset): Future[Done] = {
    val uri = Uri(dataSource.getDataPreparationURI(dataset.dataset_id))
    val request = HttpRequest(
      uri = uri,
      method = HttpMethods.DELETE,
      headers = defaultHeaders)
      .withHeaders(Authorization(headers.OAuth2BearerToken(accessToken)))

    logger.debug("Deleting the extracted dataset and statistics from the Agent on URI:{} for dataset_id: {} & dataset_name: {}", uri, dataset.dataset_id.get, dataset.name)

    Http().singleRequest(request).map(Try(_)) flatMap {
      case Success(res) =>
        res.status match {
          case StatusCodes.OK =>
            Future {
              logger.debug("Successfully deleted the dataset and statistics from the Agent on URI:{} for dataset_id: {} & dataset_name: {}", uri, dataset.dataset_id.get, dataset.name)
              Done
            }
          case _ =>
            Unmarshal(res.entity).to[String].map { body =>
              throw AgentCommunicationException(dataSource.name, dataSource.endpoint, s"The response status is ${res.status} [${request.uri}] and response body is $body")
            }
        }
      case Failure(e) =>
        throw e
    } recover {
      case e: Exception =>
        throw AgentCommunicationException(dataSource.name, dataSource.endpoint, "Exception while connecting to the Agent for deleting the extracted dataset and statistics", e)
    }
  }
}
