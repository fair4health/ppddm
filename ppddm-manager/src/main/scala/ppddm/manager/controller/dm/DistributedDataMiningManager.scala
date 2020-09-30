package ppddm.manager.controller.dm

import akka.http.scaladsl.model._
import com.typesafe.scalalogging.Logger
import ppddm.core.rest.model._
import ppddm.manager.client.AgentClient
import ppddm.manager.exception.{AgentCommunicationException, DataIntegrityException}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
 * This object handles the interaction with the Agents/Datasources so that the distributed data mining algorithms can be
 * executed on each Agent and the AlgorithmModels are then retrieved from the Agents through a different interaction
 * asynchronously.
 */
object DistributedDataMiningManager {

  private val logger: Logger = Logger(this.getClass)

  /**
   * This function invokes the data mining endpoint (algorithm execution) of each Agent corresponding to the selected
   * Agents (DatasetSources) of the Dataset.
   *
   * The call on the Agents are in parallel for the given dataMiningModel.
   *
   * @param dataMiningModel The DataMiningModel for which the algorithm execution endpoint of the agents will be invoked
   * @return A new DataMiningModel populated with the invoked DataMiningSource objects.
   */
  def invokeAgentsDataMining(dataMiningModel: DataMiningModel): Future[DataMiningModel] = {
    if (dataMiningModel.dataset.dataset_sources.isEmpty || dataMiningModel.dataset.dataset_sources.get.isEmpty) {
      // This is a data integrity problem. This should not happen!
      val msg = s"You want me to execute data mining for this DataMiningModel with model_id:${dataMiningModel.model_id} and " +
        s"name:${dataMiningModel.name} HOWEVER there are no DatasetSources in the Dataset of this DataMiningModel. " +
        s"dataset_id:${dataMiningModel.dataset.dataset_id.get} and dataset_name:${dataMiningModel.dataset.name}"
      throw DataIntegrityException(msg)
    }

    // Find the Agents to be connected for data mining (those are the SELECTED ones for the Dataset)
    val agents = dataMiningModel.dataset.dataset_sources.get
      .filter(_.selection_status.get == SelectionStatus.SELECTED)
      .map(_.agent)

    logger.debug("I will invoke the data mining endpoints of the agents with agent-ids: {}",
      agents.map(_.agent_id).mkString(","))

    Future.sequence(
      agents.map(invokeAlgorithmExecution(_, dataMiningModel))
    ) map { responses =>
      val failedAgents = responses.collect { case Failure(x) => x }
      if (failedAgents.nonEmpty) {
        logger.error("There are {} Agents out of {} which returned error on algorithm execution (data mining) request.", failedAgents.size, responses.size)
        failedAgents.foreach(logger.error("Error during Agent communication for algorithm execution (data mining)", _))
      }

      val successfulAgents = responses.collect { case Success(x) => x }
      if (successfulAgents.isEmpty) {
        val msg = "No Agents are communicated, hence I cannot create a DataminingModel!!"
        throw AgentCommunicationException("All Agents", "", msg)
      }
      dataMiningModel
        .withDataMiningSources(successfulAgents) // create a new DataMiningModel with the DataMiningSources which are EXECUTING
    }
  }

  /**
   * Invokes the algorithm execution (data mining) endpoint of the given Agent for the given DataMiningModel
   *
   * @param agent
   * @param dataMiningModel
   * @return
   */
  private def invokeAlgorithmExecution(agent: Agent, dataMiningModel: DataMiningModel): Future[Try[DataMiningSource]] = {
    val algorithmExecutionRequest = AlgorithmExecutionRequest(dataMiningModel.model_id.get, dataMiningModel.dataset.dataset_id.get,
      agent, dataMiningModel.algorithms, dataMiningModel.created_by)

    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.POST, agent.getDataMiningURI(), Some(algorithmExecutionRequest))

    logger.debug("Invoking agent data data mining (algorithm execution) on URI:{} for model_id: {} & model_name: {}",
      agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)

    AgentClient.invokeHttpRequest[DataMiningSource](agentRequest) map { result =>
      logger.debug("Agent data mining (algorithm execution) invocation successful on URI:{} for model_id: {} & model_name: {}",
        agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)
      result
    }

  }

}
