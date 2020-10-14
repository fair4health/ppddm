package ppddm.manager.controller.dm

import akka.Done
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
    val agents = DataMiningModelController.getSelectedAgents(dataMiningModel)

    logger.debug("I will invoke the data mining endpoints of the agents with agent-ids: {}",
      agents.map(_.agent_id).mkString(","))

    Future.sequence(agents.map(invokeModelTraining(_, dataMiningModel))) map { responses =>
      val failedAgents = responses.collect { case Failure(x) => x }
      if (failedAgents.nonEmpty) {
        logger.error("There are {} Agents out of {} which returned error on model training request.", failedAgents.size, responses.size)
        failedAgents.foreach(logger.error("Error during Agent communication for model training request", _))
      }

      val successfulAgents = responses.collect { case Success(x) => x }
      if (successfulAgents.isEmpty) {
        val msg = "No Agents are communicated for model training!!"
        throw AgentCommunicationException("Model Training Request: No Agents are communicated.", "", msg)
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
  private def getModelTrainingResult(agent: Agent, dataMiningModel: DataMiningModel): Future[Option[ModelTrainingResult]] = {
    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.GET, agent.getTrainingURI(dataMiningModel.model_id))

    logger.debug("Asking the ModelTrainingResult to the Agent with id:{} on URI:{} for model_id: {} & model_name: {}",
      agent.agent_id, agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)

    AgentClient.invokeHttpRequest[ModelTrainingResult](agentRequest).map(_.toOption)
  }

  def askAgentsModelTrainingResults(dataMiningModel: DataMiningModel): Future[Seq[ModelTrainingResult]] = {
    // TODO: Implement
    null
  }

  /**
   * Asks the data mining (algorithm execution) results of the DataMiningModel to the Agents.
   *
   * @param dataMiningModel
   * @return A new DataMiningModel which contains the new DataMiningSources based on the retrieved results
   */
  def askAgentsDataMiningResults(dataMiningModel: DataMiningModel): Future[DataMiningModel] = {
    Future.apply(dataMiningModel)
    //    if (dataMiningModel.data_mining_sources.isEmpty || dataMiningModel.data_mining_sources.get.isEmpty) {
    //      val msg = s"You want me to ask the data mining (algorithm execution) results of this DataMiningModel with " +
    //        s"model_id:${dataMiningModel.model_id} and name:${dataMiningModel.name} HOWEVER there are no DataMiningSources for this DataMiningModel"
    //      throw DataIntegrityException(msg)
    //    }
    //
    //    if (dataMiningModel.execution_state.get != ExecutionState.EXECUTING) {
    //      // If the Dataset is not executing any queries, then it means all Agents returned their responses.
    //      // There is no need to ask any questions to the Dataset sources (Agents)
    //      logger.debug("There is no need to ask the AlgorithmExecutionResults of the DataMiningModel with id:{} and name:{}. Its state is {}",
    //        dataMiningModel.model_id, dataMiningModel.name, dataMiningModel.execution_state.get)
    //      Future.apply(dataMiningModel)
    //    } else {
    //      logger.debug("I will ask for the AlgorithmExecutionResults from {} DataMiningSources of the DataMiningModel with id:{} and name:{}",
    //        dataMiningModel.data_mining_sources.get.size, dataMiningModel.model_id, dataMiningModel.name)
    //
    //      Future.sequence(
    //        dataMiningModel.data_mining_sources.get.map { dataMiningSource: DataMiningSource => // For each dataMiningSource in this set (actually, for each Agent)
    //          getAlgorithmExecutionResult(dataMiningSource.agent, dataMiningModel) // Ask for the algorithm execution result (do this in parallel)
    //        }
    //      ) map { responses: Seq[Option[ModelTrainingResult]] => // Join the responses coming from different Agents
    //        logger.debug("AlgorithmExecutionResults have been retrieved from all {} Agents of the dataMiningMOdel.", responses.size)
    //        responses.map(result => { // For each AlgorithmExecutionResult
    //          result map { algorithmExecutionResult => // Create a corresponding DataMiningSource object
    //            DataMiningSource(algorithmExecutionResult.agent, Some(algorithmExecutionResult.algorithm_models), Some(ExecutionState.FINAL))
    //          }
    //        })
    //          .filter(_.isDefined) // Keep the the Agents which produced the results
    //          .map(_.get) // Get rid of the Option since we eliminated the None elements above
    //      } map { dataMiningSourcesWithResult: Seq[DataMiningSource] => // DataMiningSources which finished algorithm execution (data mining)
    //        val updatedDataMiningSources = dataMiningModel.data_mining_sources.get map { existingDataMiningSource => // Iterate over the existing DataMiningSources of the dataMiningModel
    //          // decide whether there is an algorithm execution result for the existingDataMiningSource
    //          val finishedDataMiningSource = dataMiningSourcesWithResult.find(_.agent.agent_id == existingDataMiningSource.agent.agent_id)
    //          // Return the DataMiningSource if that has a result, otherwise keep the existing DataMiningSource in the list
    //          if (finishedDataMiningSource.isDefined) finishedDataMiningSource.get else existingDataMiningSource
    //        }
    //        dataMiningModel.withDataMiningSources(updatedDataMiningSources) // create a new DataMiningModel with the updatedDatasetSources
    //      }
    //    }

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
