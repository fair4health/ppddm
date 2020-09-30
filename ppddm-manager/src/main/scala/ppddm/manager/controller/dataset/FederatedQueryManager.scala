package ppddm.manager.controller.dataset

import akka.Done
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Accept, Authorization}
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.typesafe.scalalogging.Logger
import ppddm.core.rest.model._
import ppddm.manager.client.AgentClient
import ppddm.manager.exception.{AgentCommunicationException, DataIntegrityException}
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
      AgentRegistry.agents.map { agent =>
        invokeDataPreparation(agent, dataset)
      }
    ) map { responses =>
      val failedAgents = responses.collect { case Failure(x) => x }
      if (failedAgents.nonEmpty) {
        logger.error("There are {} agents (data sources) out of {} which returned error on data preparation request.", failedAgents.size, responses.size)
        failedAgents.foreach(logger.error("Error during Agent communication for data preparation", _))
      }

      val successfulAgents = responses.collect { case Success(x) => x }
      if (successfulAgents.isEmpty) {
        val msg = "No Agents are communicated, hence I cannot create a Dataset!!"
        throw AgentCommunicationException("All Agents", "", msg)
      }
      dataset
        .withDatasetSources(successfulAgents) // create a new Dataset with the DatasetSources which are EXECUTING
    }
  }

  /**
   * Invokes the data preparation endpoint of the given Agent for the given Dataset
   *
   * @param agent
   * @param dataset
   * @return
   */
  private def invokeDataPreparation(agent: Agent, dataset: Dataset): Future[Try[DatasetSource]] = {
    val dataPreparationRequest: DataPreparationRequest = DataPreparationRequest(dataset.dataset_id.get, agent,
      dataset.featureset, dataset.eligibility_criteria, dataset.created_by)

    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.POST, agent.getDataMiningURI(), Some(dataPreparationRequest))

    logger.debug("Invoking agent data preparation on URI:{} for dataset_id: {} & dataset_name: {}",
      agentRequest.httpRequest.getUri(), dataset.dataset_id.get, dataset.name)

    AgentClient.invokeHttpRequest[DatasetSource](agentRequest) map { result =>
      logger.debug("Agent data preparation invocation successful on URI:{} for dataset_id: {} & dataset_name: {}",
        agentRequest.httpRequest.getUri(), dataset.dataset_id.get, dataset.name)
      result
    }
  }

  /**
   * Asks the data preparation results of the dataset to the DatasetSources defined within the given dataset.
   *
   * @param dataset
   * @return A new Dataset which contains the new DatasetSources based on the retrieved results
   */
  def askAgentsDataPreparationResults(dataset: Dataset): Future[Dataset] = {
    if (dataset.dataset_sources.isEmpty || dataset.dataset_sources.get.isEmpty) {
      val msg = s"You want me to ask the data preparation results of this dataset with id:${dataset.dataset_id} and " +
        s"name:${dataset.name} HOWEVER there are no DatasetSources for this Dataset"
      throw DataIntegrityException(msg)
    }

    if (dataset.execution_state.get != ExecutionState.EXECUTING) {
      // If the Dataset is not executing any queries, then it means all Agents returned their responses.
      // There is no need to ask any questions to the Dataset sources (Agents)
      logger.debug("There is no need to ask the DataPreparationResults of the Dataset with id:{} and name:{}. Its state is {}",
        dataset.dataset_id, dataset.name, dataset.execution_state.get)
      Future.apply(dataset)
    } else {
      logger.debug("I will ask for the DataPreparationResults from {} DatasetSources of the Dataset with id:{} and name:{}",
        dataset.dataset_sources.get.size, dataset.dataset_id, dataset.name)

      Future.sequence(
        dataset.dataset_sources.get.map { datasetSource: DatasetSource => // For each datasetSource in this set (actually, for each DataSource)
          getPreparedDataStatistics(datasetSource.agent, dataset) // Ask for the data preparation results (do this in parallel)
        }
      ) map { responses: Seq[Option[DataPreparationResult]] => // Join the responses coming from different data sources (Agents)
        logger.debug("DataPreparationResults have been retrieved from all {} data sources (Agents) of the dataset.", responses.size)
        responses.map(result => { // For each DataPreparationResult
          result map { dataPreparationResult => // Create a corresponding DatasetSource object
            DatasetSource(dataPreparationResult.agent, Some(dataPreparationResult.agent_data_statistics), None, Some(ExecutionState.FINAL))
          }
        })
          .filter(_.isDefined) // Keep the the data sources which produced the results
          .map(_.get) // Get rid of the Option since we eliminated the None elements above
      } map { datasetSourcesWithResult: Seq[DatasetSource] => // DatasetSources which finished data preparation
        val updatedDatasetSources = dataset.dataset_sources.get map { existingDatasetSource => // Iterate over the existing DatasetSources of the dataset
          // decide whether there is a data preparation result for the existingDatasetSource
          val finishedDatasetSource = datasetSourcesWithResult.find(_.agent.agent_id == existingDatasetSource.agent.agent_id)
          // Return the DatasetSource if that has a result, otherwise keep the existing DatasetSource in the list
          if (finishedDatasetSource.isDefined) finishedDatasetSource.get else existingDatasetSource
        }
        dataset.withDatasetSources(updatedDatasetSources) // create a new Dataset with the updatedDatasetSources
      }
    }
  }

  /**
   * Asks the DataPreparationResult from the given Agent for the given dataset.
   *
   * @param agent
   * @param dataset
   * @return An Option[DataPreparationResult]. If the result is None, that means the data has not been prepared yet.
   */
  private def getPreparedDataStatistics(agent: Agent, dataset: Dataset): Future[Option[DataPreparationResult]] = {
    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.GET, agent.getDataPreparationURI(dataset.dataset_id))

    logger.debug("Asking the data preparation result to the Agent on URI:{} for dataset_id: {} & dataset_name: {}",
      agentRequest.httpRequest.getUri(), dataset.dataset_id.get, dataset.name)

    AgentClient.invokeHttpRequest[DataPreparationResult](agentRequest).map(_.toOption)
  }

  /**
   * Deletes the extracted Dataset and Statistics from the Agents.
   *
   * @param agent
   * @param dataset
   * @return
   */
  def deleteDatasetAndStatistics(agent: Agent, dataset: Dataset): Future[Done] = {
    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.DELETE, agent.getDataPreparationURI(dataset.dataset_id))

    logger.debug("Deleting the extracted dataset and statistics from the Agent on URI:{} for dataset_id: {} & dataset_name: {}",
      agentRequest.httpRequest.getUri(), dataset.dataset_id.get, dataset.name)

    AgentClient.invokeHttpRequest[Done](agentRequest) map {
      case Success(result) =>
        logger.debug("Successfully deleted the dataset and statistics from the Agent on URI:{} for dataset_id: {} & dataset_name: {}",
          agentRequest.httpRequest.getUri(), dataset.dataset_id.get, dataset.name)
        result
      case Failure(ex) => throw ex
    }
  }
}
