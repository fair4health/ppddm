package ppddm.manager.controller.dm

import akka.Done
import akka.http.scaladsl.model._
import com.typesafe.scalalogging.Logger
import ppddm.core.rest.model._
import ppddm.manager.client.AgentClient
import ppddm.manager.exception.AgentCommunicationException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
 * This object handles the interaction with the Agents/Datasources so that the distributed data mining algorithms can be
 * trained, validated and tested on each Agent controlled by an orchestration handled by the DataMiningOrchestrator.
 */
object DistributedDataMiningManager {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Invokes the model training endpoint of the given Agent for the given DataMiningModel
   *
   * @param agent
   * @param dataMiningModel
   * @return
   */
  private def invokeModelTraining(agent: Agent, dataMiningModel: DataMiningModel): Future[Try[Done]] = {
    val modelTrainingRequest = ModelTrainingRequest(dataMiningModel.model_id.get, dataMiningModel.dataset.dataset_id.get,
      agent, dataMiningModel.algorithms, dataMiningModel.created_by)

    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.POST, agent.getTrainingURI(), Some(modelTrainingRequest))

    logger.debug("Invoking agent model training on URI:{} for model_id: {} & model_name: {}",
      agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)

    AgentClient.invokeHttpRequest[Done](agentRequest) map { result =>
      logger.debug("Agent model training invocation successful on URI:{} for model_id: {} & model_name: {}",
        agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)
      result
    }
  }

  /**
   * This function invokes the model training endpoint of each Agent corresponding to the selected
   * Agents (DatasetSources) of the Dataset.
   *
   * The call on the Agents are in parallel for the given dataMiningModel.
   *
   * @param dataMiningModel The DataMiningModel for which the algorithm execution endpoint of the agents will be invoked
   * @return
   */
  def invokeAgentsModelTraining(dataMiningModel: DataMiningModel): Future[Done] = {
    // Get all Agents of this DataMiningModel to which model training requests will be POSTed
    val agents = DataMiningModelController.getSelectedAgents(dataMiningModel)

    logger.debug("I will invoke the model training endpoints of {} agents with agent-ids: {}",
      agents.length, agents.map(_.agent_id).mkString(","))

    Future.sequence(agents.map(invokeModelTraining(_, dataMiningModel))) map { responses =>
      val failedAgents = responses.collect { case Failure(x) => x }
      if (failedAgents.nonEmpty) {
        val msg = s"There are ${failedAgents.size} Agents out of ${responses.size} which returned error on model training request."
        logger.error(msg)
        throw AgentCommunicationException(reason = msg)
      }

      Done
    }
  }

  /**
   * Asks the ModelTrainingResult from the given Agent for the given dataMiningModel.
   *
   * @param agent
   * @param dataMiningModel
   * @return An Option[ModelTrainingResult]. If the result is None, that means the model training has not completed yet.
   */
  private def getModelTrainingResultFromAgent(agent: Agent, dataMiningModel: DataMiningModel): Future[Option[ModelTrainingResult]] = {
    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.GET, agent.getTrainingURI(dataMiningModel.model_id))

    logger.debug("Asking the ModelTrainingResult to the Agent with id:{} on URI:{} for model_id: {} & model_name: {}",
      agent.agent_id, agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)

    AgentClient.invokeHttpRequest[ModelTrainingResult](agentRequest).map(_.toOption)
  }

  /**
   * Asks the model training results of the DataMiningModel to the Agents. These Agents were previously POSTed to
   * start the trainings on their datasets. And only the Agents whose results were not received yet are POSTed.
   *
   * @param dataMiningModel
   * @return Returns a sequence of ModelTrainingResult. Only the results of Agents which finished their model trainings will be returned by this function.
   */
  def askAgentsModelTrainingResults(dataMiningModel: DataMiningModel): Future[Seq[ModelTrainingResult]] = {
    // Get the Agents whose ModelTrainingResults have not been received yet
    val agents = DataMiningModelController.getAgentsWaitedForTrainingResults(dataMiningModel)

    logger.debug("I will ask the model training results to {} agents with agent-ids: {}",
      agents.length, agents.map(_.agent_id).mkString(","))

    Future.sequence(agents.map(getModelTrainingResultFromAgent(_, dataMiningModel))) map { responses =>
      responses
        .filter(_.isDefined) // keep only ready ModelTrainingResults
        .map(_.get) // get rid of Option
    }
  }

  /**
   * Deletes the trained AlgorithmExecutionResults from the Agents.
   *
   * @param agent
   * @param dataMiningModel
   * @return
   */
  def deleteAlgorithmExecutionResult(agent: Agent, dataMiningModel: DataMiningModel): Future[Done] = {
    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.DELETE, agent.getTrainingURI(dataMiningModel.model_id))

    logger.debug("Deleting the algorithm execution result from the Agent on URI:{} for model_id: {} & model_name: {}",
      agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)

    AgentClient.invokeHttpRequest[Done](agentRequest) map {
      case Success(result) =>
        logger.debug("Successfully deleted the algorithm execution result from the Agent on URI:{} for model_id: {} & model_name: {}",
          agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)
        result
      case Failure(ex) => throw ex
    }
  }

}
