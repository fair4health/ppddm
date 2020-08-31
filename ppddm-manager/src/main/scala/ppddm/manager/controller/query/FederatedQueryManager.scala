package ppddm.manager.controller.query

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{Accept, Authorization}
import akka.http.scaladsl.model._
import com.typesafe.scalalogging.Logger
import ppddm.core.rest.model._
import ppddm.core.util.URLUtil
import ppddm.manager.exception.AgentCommunicationException
import ppddm.manager.registry.AgentRegistry

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

/* To use the toJson, toPrettyJson methods of the JsonFormatter */
import ppddm.core.util.JsonFormatter._

/* Import the ActorSystem */
import ppddm.manager.config.ManagerExecutionContext._

/**
 * This object handles the interaction with the Agents/Datasources so that the fedarated data preparation queries can be
 * executed on each Datasource endpoint.
 */
object FederatedQueryManager {

  val DATA_PREPARATION_PATH = "prepare"

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
    logger.debug("Will invoke the prepare endpoints of the registered agents")

    Future.sequence(AgentRegistry.dataSources.map { dataSource =>
      invokeDataPreparation(dataSource, dataset)
    }) map { responses =>
      val successfulAgents = responses
        .filter(_.isDefined) // Filter only the Nonempty responses
        .map(_.get) // Convert them to Nonoption objects
      val unsuccessfulAgentSize = AgentRegistry.dataSources.size - successfulAgents.size
      if (unsuccessfulAgentSize > 0) {
        logger.warn("There are {} agents (data sources) which returned error on data preparation request.", unsuccessfulAgentSize)
      }
      successfulAgents
    } map { datasetSources =>
      dataset
        .withDataSources(datasetSources) // create a new Dataset with the DatasetSources which are QUERYING
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
  private def invokeDataPreparation(dataSource: DataSource, dataset: Dataset): Future[Option[DatasetSource]] = {
    val dataPreparationRequest: DataPreparationRequest = DataPreparationRequest(dataset.dataset_id.get, dataset.featureset, dataset.eligibility_criteria, dataset.created_by)
    val uri = URLUtil.append(dataSource.endpoint, DATA_PREPARATION_PATH)

    val request = HttpRequest(
      uri = Uri(uri),
      method = HttpMethods.POST,
      headers = defaultHeaders)
      .withHeaders(Authorization(headers.OAuth2BearerToken(accessToken)))
      .withEntity(ContentTypes.`application/json`, dataPreparationRequest.toJson)

    logger.debug("Invoking agent data preparation on URI:{} for dataset_id: {} & dataset_name: {}", uri, dataset.dataset_id, dataset.name)

    Http().singleRequest(request) map {
      case resp if resp.status == StatusCodes.OK =>
        logger.debug("Agent data preparation invocation successful on URI:{} for dataset_id: {} & dataset_name: {}", uri, dataset.dataset_id, dataset.name)
        Some(DatasetSource(dataSource, None, None, Some(ExecutionState.QUERYING)))
      case errUnk =>
        errUnk.entity.toStrict(FiniteDuration(1000, MILLISECONDS)).map(_.data.utf8String).map { entity =>
          logger.error(entity)
          throw AgentCommunicationException(entity)
        }
        None
    }

  }

  def getPreparedDataStatistics(dataset_id: String): Future[Option[DataPreparationResult]] = {
    Future {
      None
    }
  }
}
