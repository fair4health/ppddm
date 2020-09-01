package ppddm.manager.controller.query

import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Accept, Authorization}
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.typesafe.scalalogging.Logger
import ppddm.core.rest.model._
import ppddm.core.util.URLUtil
import ppddm.manager.exception.AgentCommunicationException
import ppddm.manager.registry.AgentRegistry

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

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
    val dataPreparationRequest: DataPreparationRequest = DataPreparationRequest(dataset.dataset_id.get, dataset.featureset, dataset.eligibility_criteria, dataset.created_by)
    val uri = URLUtil.append(dataSource.endpoint, DATA_PREPARATION_PATH)

    val request = HttpRequest(
      uri = Uri(uri),
      method = HttpMethods.POST,
      headers = defaultHeaders)
      .withHeaders(Authorization(headers.OAuth2BearerToken(accessToken)))
      .withEntity(ContentTypes.`application/json`, dataPreparationRequest.toJson)

    logger.debug("Invoking agent data preparation on URI:{} for dataset_id: {} & dataset_name: {}", uri, dataset.dataset_id, dataset.name)

    Http().singleRequest(request).map(Try(_)) flatMap {
      case Success(res) =>
        res.status match {
          case StatusCodes.OK =>
            logger.debug("Agent data preparation invocation successful on URI:{} for dataset_id: {} & dataset_name: {}", uri, dataset.dataset_id, dataset.name)
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
        Failure(AgentCommunicationException(dataSource.name, dataSource.endpoint, "Exception while connecting to the Agent",e))
    }
  }

  def getPreparedDataStatistics(dataset_id: String): Future[Option[DataPreparationResult]] = {
    Future {
      None
    }
  }
}
