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

  // ******* TRAINING *******

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
   * @param dataMiningModel
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

    logger.debug("Asking the ModelTrainingResult to the Agent with agent_id:{} on URI:{} for model_id: {} & model_name: {}",
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
   * Deletes the training results of the DataMiningModel indicated by the model_id on the given Agent
   *
   * @param agent
   * @param dataMiningModel
   * @return
   */
  private def deleteModelTrainingResultFromAgent(agent: Agent, dataMiningModel: DataMiningModel): Future[Option[Done]] = {
    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.DELETE, agent.getTrainingURI(dataMiningModel.model_id))

    logger.debug("Deleting the ModelTrainingResult on the Agent with agent_id:{} on URI:{} for model_id: {} & model_name: {}",
      agent.agent_id, agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)

    AgentClient.invokeHttpRequest[Done](agentRequest).map(_.toOption)
  }

  /**
   * Sends DELETE requests for the training results to all selected Agents of the given DataMiningModel.
   *
   * @param dataMiningModel
   * @return
   */
  def deleteAgentsModelTrainingResults(dataMiningModel: DataMiningModel): Future[Done] = {
    val agents = DataMiningModelController.getSelectedAgents(dataMiningModel)

    logger.debug("I will invoke the DELETE model training endpoints of {} agents with agent-ids: {}",
      agents.length, agents.map(_.agent_id).mkString(","))

    Future.sequence(agents.map(deleteModelTrainingResultFromAgent(_, dataMiningModel))) map { responses =>
      if (responses.exists(_.isEmpty)) {
        logger.warn("We are trying to delete the training results on Agents, however; there is an Agent whose training results were not there!")
      }
      Done
    }
  }

  // ******* VALIDATION *******

  /**
   * Invoke the model validation endpoint of the given agent for the given weakModels.
   *
   * @param agent
   * @param weakModels
   * @param dataMiningModel
   * @return
   */
  private def invokeModelValidation(agent: Agent, weakModels: Seq[WeakModel], dataMiningModel: DataMiningModel): Future[Try[Done]] = {
    val modelValidationRequest = ModelValidationRequest(dataMiningModel.model_id.get, dataMiningModel.dataset.dataset_id.get, agent, weakModels, dataMiningModel.created_by)

    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.POST, agent.getValidationURI(), Some(modelValidationRequest))

    logger.debug("Invoking agent model validation on URI:{} for {} number of WeakModels, for model_id: {} & model_name: {}",
      agentRequest.httpRequest.getUri(), weakModels.length, dataMiningModel.model_id.get, dataMiningModel.name)

    AgentClient.invokeHttpRequest[Done](agentRequest) map { result =>
      logger.debug("Agent model validation invocation successful on URI:{} for {} number of WeakModels, for model_id: {} & model_name: {}",
        agentRequest.httpRequest.getUri(), weakModels.length, dataMiningModel.model_id.get, dataMiningModel.name)
      result
    }
  }

  /**
   * This function sends each WeakModel trained in an Agent to the validation endpoints of the other Agents so that
   * the fitted_models of Agents are validated on remaining Agents.
   *
   * @param dataMiningModel
   * @return
   */
  def invokeAgentsModelValidation(dataMiningModel: DataMiningModel): Future[Done] = {
    // Get the (Agents, Seq[WeakModel) pairs for validation
    val agentValidationPairs = DataMiningModelController.getAgentValidationModelPairs(dataMiningModel)

    logger.debug(s"I will invoke the model validation endpoints of ${agentValidationPairs.length} agents with the following details:")
    agentValidationPairs.foreach(pair => logger.debug(s"Agent:${pair._1.name} will validate the WeakModels for ${pair._2.map(_.algorithm.name).mkString(",")}"))

    Future.sequence(agentValidationPairs.map(pair => invokeModelValidation(pair._1, pair._2, dataMiningModel))) map { responses =>
      val failedAgents = responses.collect { case Failure(x) => x }
      if (failedAgents.nonEmpty) {
        val msg = s"There are ${failedAgents.size} Agents out of ${responses.size} which returned error on model validation request."
        logger.error(msg)
        throw AgentCommunicationException(reason = msg)
      }

      Done
    }
  }

  /**
   * Asks the ModelValidationResult from the given Agent for the given dataMiningModel.
   *
   * @param agent
   * @param dataMiningModel
   * @return An Option[ModelValidationResult]. If the result is None, that means the model validation has not completed yet.
   */
  private def getModelValidationResultFromAgent(agent: Agent, dataMiningModel: DataMiningModel): Future[Option[ModelValidationResult]] = {
    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.GET, agent.getValidationURI(dataMiningModel.model_id))

    logger.debug("Asking the ModelValidationResult to the Agent with id:{} on URI:{} for model_id: {} & model_name: {}",
      agent.agent_id, agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)

    AgentClient.invokeHttpRequest[ModelValidationResult](agentRequest).map(_.toOption)
  }

  /**
   * Asks the model validation results of the DataMiningModel to the Agents. These Agents were previously POSTed to
   * start validating the WeakModels (these WeakModels were trained on other Agents) on their datasets.
   * And only the Agents whose results were not received yet are POSTed.
   *
   * @param dataMiningModel
   * @return Returns a sequence of ModelValidationResult. Only the results of Agents which finished their model validation will be returned by this function.
   */
  def askAgentsModelValidationResults(dataMiningModel: DataMiningModel): Future[Seq[ModelValidationResult]] = {
    // Get the Agents whose ModelTrainingResults have not been received yet
    val agents = DataMiningModelController.getAgentsWaitedForValidationResults(dataMiningModel)

    logger.debug("I will ask the model validation results to {} agents with agent-ids: {}",
      agents.length, agents.map(_.agent_id).mkString(","))

    Future.sequence(agents.map(getModelValidationResultFromAgent(_, dataMiningModel))) map { responses =>
      responses
        .filter(_.isDefined) // keep only ready ModelValidationResult
        .map(_.get) // get rid of Option
    }
  }

  /**
   * Deletes the validation results of the DataMiningModel indicated by the model_id on the given Agent
   *
   * @param agent
   * @param dataMiningModel
   * @return
   */
  private def deleteModelValidationResultFromAgent(agent: Agent, dataMiningModel: DataMiningModel): Future[Option[Done]] = {
    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.DELETE, agent.getValidationURI(dataMiningModel.model_id))

    logger.debug("Deleting the ModelValidationResult on the Agent with agent_id:{} on URI:{} for model_id: {} & model_name: {}",
      agent.agent_id, agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)

    AgentClient.invokeHttpRequest[Done](agentRequest).map(_.toOption)
  }

  /**
   * Sends DELETE requests for the validation results to all selected Agents of the given DataMiningModel.
   *
   * @param dataMiningModel
   * @return
   */
  def deleteAgentsModelValidationResults(dataMiningModel: DataMiningModel): Future[Done] = {
    val agents = DataMiningModelController.getSelectedAgents(dataMiningModel)

    logger.debug("I will invoke the DELETE model validation endpoints of {} agents with agent-ids: {}",
      agents.length, agents.map(_.agent_id).mkString(","))

    Future.sequence(agents.map(deleteModelValidationResultFromAgent(_, dataMiningModel))) map { responses =>
      if (responses.exists(_.isEmpty)) {
        logger.warn("We are trying to delete the validation results on Agents, however; there is an Agent whose validation results were not there!")
      }
      Done
    }
  }

  // ******* TESTING *******

  /**
   * Invokes the model testing endpoint of the given Agent for the given DataMiningModel
   *
   * @param agent
   * @param dataMiningModel
   * @return
   */
  private def invokeModelTesting(agent: Agent, dataMiningModel: DataMiningModel): Future[Try[Done]] = {
    val modelTestRequest = ModelTestRequest(dataMiningModel.model_id.get, dataMiningModel.dataset.dataset_id.get,
      agent, dataMiningModel.boosted_models.get, dataMiningModel.created_by)

    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.POST, agent.getTestURI(), Some(modelTestRequest))

    logger.debug("Invoking agent model testing on URI:{} for model_id: {} & model_name: {}",
      agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)

    AgentClient.invokeHttpRequest[Done](agentRequest) map { result =>
      logger.debug("Agent model test invocation successful on URI:{} for model_id: {} & model_name: {}",
        agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)
      result
    }
  }

  /**
   * This function invokes the model testing endpoint of each Agent corresponding to the selected
   * Agents (DatasetSources) of the Dataset by sending all BoostedModels for testing of the trained and validated models.
   *
   * The call on the Agents are in parallel for the given dataMiningModel.
   *
   * @param dataMiningModel
   * @return
   */
  def invokeAgentsModelTesting(dataMiningModel: DataMiningModel): Future[Done] = {
    // Get all Agents of this DataMiningModel to which model test requests will be POSTed
    val agents = DataMiningModelController.getSelectedAgents(dataMiningModel)

    logger.debug("I will invoke the model test endpoints of {} agents with agent-ids: {} for {} number of BoostedModels " +
      "where the Algorithms are {}",
      agents.length, agents.map(_.agent_id).mkString(","), dataMiningModel.boosted_models.get.length, dataMiningModel.boosted_models.get.map(_.algorithm.name).mkString(","))

    Future.sequence(agents.map(invokeModelTesting(_, dataMiningModel))) map { responses =>
      val failedAgents = responses.collect { case Failure(x) => x }
      if (failedAgents.nonEmpty) {
        val msg = s"There are ${failedAgents.size} Agents out of ${responses.size} which returned error on model test request."
        logger.error(msg)
        throw AgentCommunicationException(reason = msg)
      }

      Done
    }
  }

  /**
   * Asks the ModelTestResult from the given Agent for the given dataMiningModel.
   *
   * @param agent
   * @param dataMiningModel
   * @return An Option[ModelTestResult]. If the result is None, that means the model testing has not completed yet.
   */
  private def getModelTestResultFromAgent(agent: Agent, dataMiningModel: DataMiningModel): Future[Option[ModelTestResult]] = {
    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.GET, agent.getTestURI(dataMiningModel.model_id))

    logger.debug("Asking the ModelTestResult to the Agent with id:{} on URI:{} for model_id: {} & model_name: {}",
      agent.agent_id, agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)

    AgentClient.invokeHttpRequest[ModelTestResult](agentRequest).map(_.toOption)
  }

  /**
   * Asks the model test results of the DataMiningModel to the Agents. These Agents were previously POSTed to
   * start testing the BoostedModels on their datasets.
   * And only the Agents whose results were not received yet are POSTed.
   *
   * @param dataMiningModel
   * @return Returns a sequence of ModelTestResult. Only the results of Agents which finished their model testing will be returned by this function.
   */
  def askAgentsModelTestResults(dataMiningModel: DataMiningModel): Future[Seq[ModelTestResult]] = {
    // Get the Agents whose ModelTestResults have not been received yet
    val agents = DataMiningModelController.getAgentsWaitedForTestResults(dataMiningModel)

    logger.debug("I will ask the model test results to {} agents with agent-ids: {}",
      agents.length, agents.map(_.agent_id).mkString(","))

    Future.sequence(agents.map(getModelTestResultFromAgent(_, dataMiningModel))) map { responses =>
      responses
        .filter(_.isDefined) // keep only ready ModelTestResult
        .map(_.get) // get rid of Option
    }
  }

  /**
   * Deletes the test results of the DataMiningModel indicated by the model_id on the given Agent
   *
   * @param agent
   * @param dataMiningModel
   * @return
   */
  private def deleteModelTestResultFromAgent(agent: Agent, dataMiningModel: DataMiningModel): Future[Option[Done]] = {
    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.DELETE, agent.getTestURI(dataMiningModel.model_id))

    logger.debug("Deleting the ModelTestResult on the Agent with agent_id:{} on URI:{} for model_id: {} & model_name: {}",
      agent.agent_id, agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)

    AgentClient.invokeHttpRequest[Done](agentRequest).map(_.toOption)
  }

  /**
   * Sends DELETE requests for the test results to all selected Agents of the given DataMiningModel.
   *
   * @param dataMiningModel
   * @return
   */
  def deleteAgentsModelTestResults(dataMiningModel: DataMiningModel): Future[Done] = {
    val agents = DataMiningModelController.getSelectedAgents(dataMiningModel)

    logger.debug("I will invoke the DELETE model test endpoints of {} agents with agent-ids: {}",
      agents.length, agents.map(_.agent_id).mkString(","))

    Future.sequence(agents.map(deleteModelTestResultFromAgent(_, dataMiningModel))) map { responses =>
      if (responses.exists(_.isEmpty)) {
        logger.warn("We are trying to delete the test results on Agents, however; there is an Agent whose test results were not there!")
      }
      Done
    }
  }

  // ******* FREQUENCY CALCULATION *******

  /**
   * Invokes the frequency calculation endpoint of the given Agent for the given DataMiningModel
   *
   * @param agent
   * @param dataMiningModel
   * @return
   */
  private def invokeARLFrequencyCalculation(agent: Agent, dataMiningModel: DataMiningModel): Future[Try[Done]] = {
    val arlFrequencyCalculationRequest = ARLFrequencyCalculationRequest(dataMiningModel.model_id.get, dataMiningModel.dataset.dataset_id.get,
      agent, dataMiningModel.created_by)

    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.POST, agent.getARLFrequencyCalculationURI(), Some(arlFrequencyCalculationRequest))

    logger.debug("Invoking agent frequency calculation on URI:{} for model_id: {} & model_name: {}",
      agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)

    AgentClient.invokeHttpRequest[Done](agentRequest) map { result =>
      logger.debug("Agent model frequency calculation invocation successful on URI:{} for model_id: {} & model_name: {}",
        agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)
      result
    }
  }

  /**
   * This function invokes the frequency calculation endpoint of each Agent corresponding to the selected
   * Agents (DatasetSources) of the Dataset.
   *
   * The call on the Agents are in parallel for the given dataMiningModel.
   *
   * @param dataMiningModel
   * @return
   */
  def invokeAgentsARLFrequencyCalculation(dataMiningModel: DataMiningModel): Future[Done] = {
    // Get all Agents of this DataMiningModel to which frequency calculation requests will be POSTed
    val agents = DataMiningModelController.getSelectedAgents(dataMiningModel)

    logger.debug("I will invoke the frequency calculation endpoints of {} agents with agent-ids: {} for {} number of BoostedModels " +
      "where the Algorithms are {}",
      agents.length, agents.map(_.agent_id).mkString(","), dataMiningModel.boosted_models.get.length, dataMiningModel.boosted_models.get.map(_.algorithm.name).mkString(","))

    Future.sequence(agents.map(invokeARLFrequencyCalculation(_, dataMiningModel))) map { responses =>
      val failedAgents = responses.collect { case Failure(x) => x }
      if (failedAgents.nonEmpty) {
        val msg = s"There are ${failedAgents.size} Agents out of ${responses.size} which returned error on frequency calculation request."
        logger.error(msg)
        throw AgentCommunicationException(reason = msg)
      }

      Done
    }
  }

  /**
   * Asks the ARLFrequencyCalculationResult from the given Agent for the given dataMiningModel.
   *
   * @param agent
   * @param dataMiningModel
   * @return An Option[ARLFrequencyCalculationResult]. If the result is None, that means the frequency calculation has not completed yet.
   */
  private def getARLFrequencyCalculationResultFromAgent(agent: Agent, dataMiningModel: DataMiningModel): Future[Option[ARLFrequencyCalculationResult]] = {
    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.GET, agent.getARLFrequencyCalculationURI(dataMiningModel.model_id))

    logger.debug("Asking the ARLFrequencyCalculationResult to the Agent with id:{} on URI:{} for model_id: {} & model_name: {}",
      agent.agent_id, agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)

    AgentClient.invokeHttpRequest[ARLFrequencyCalculationResult](agentRequest).map(_.toOption)
  }

  /**
   * Asks the frequency calculation results of the DataMiningModel to the Agents. These Agents were previously POSTed to
   * start calculating the item frequencies on their datasets.
   * And only the Agents whose results were not received yet are POSTed.
   *
   * @param dataMiningModel
   * @return Returns a sequence of ARLFrequencyCalculationResult. Only the results of Agents which finished their frequency calculation will be returned by this function.
   */
  def askAgentsARLFrequencyCalculationResults(dataMiningModel: DataMiningModel): Future[Seq[ARLFrequencyCalculationResult]] = {
    // Get the Agents whose ARLFrequencyCalculationResults have not been received yet
    val agents = DataMiningModelController.getAgentsWaitedForARLFrequencyCalculationResults(dataMiningModel)

    logger.debug("I will ask the frequency calculation results to {} agents with agent-ids: {}",
      agents.length, agents.map(_.agent_id).mkString(","))

    Future.sequence(agents.map(getARLFrequencyCalculationResultFromAgent(_, dataMiningModel))) map { responses =>
      responses
        .filter(_.isDefined) // keep if it is ready
        .map(_.get) // get rid of Option
    }
  }

  /**
   * Deletes the ARLFrequencyCalculationResult of the DataMiningModel indicated by the model_id on the given Agent
   *
   * @param agent
   * @param dataMiningModel
   * @return
   */
  private def deleteARLFrequencyCalculationResultFromAgent(agent: Agent, dataMiningModel: DataMiningModel): Future[Option[Done]] = {
    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.DELETE, agent.getARLFrequencyCalculationURI(dataMiningModel.model_id))

    logger.debug("Deleting the ARLFrequencyCalculationResult on the Agent with agent_id:{} on URI:{} for model_id: {} & model_name: {}",
      agent.agent_id, agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)

    AgentClient.invokeHttpRequest[Done](agentRequest).map(_.toOption)
  }

  /**
   * Sends DELETE requests for the ARL frequency calculations to all selected Agents of the given DataMiningModel.
   *
   * @param dataMiningModel
   * @return
   */
  def deleteAgentsARLFrequencyCalculationResults(dataMiningModel: DataMiningModel): Future[Done] = {
    val agents = DataMiningModelController.getSelectedAgents(dataMiningModel)

    logger.debug("I will invoke the DELETE ARL frequency calculation endpoints of {} agents with agent-ids: {}",
      agents.length, agents.map(_.agent_id).mkString(","))

    Future.sequence(agents.map(deleteARLFrequencyCalculationResultFromAgent(_, dataMiningModel))) map { responses =>
      if (responses.exists(_.isEmpty)) {
        logger.warn("We are trying to delete the ARL frequency calculation results on Agents, however; there is an Agent whose results were not there!")
      }
      Done
    }
  }

  // ******* ARL EXECUTION *******

  /**
   * Invokes the ARL execution endpoint of the given Agent for the given DataMiningModel
   *
   * @param agent
   * @param dataMiningModel
   * @return
   */
  private def invokeARLExecution(agent: Agent, dataMiningModel: DataMiningModel): Future[Try[Done]] = {
    val algorithmItemSets = DataMiningModelController.getAlgorithmItemSetsForARLExecution(dataMiningModel)
    val arlExecutionRequest = ARLExecutionRequest(dataMiningModel.model_id.get, dataMiningModel.dataset.dataset_id.get,
      agent, algorithmItemSets, dataMiningModel.created_by)

    val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.POST, agent.getARLExecutionURI(), Some(arlExecutionRequest))

    logger.debug("Invoking agent ARL execution on URI:{} for model_id: {} & model_name: {}",
      agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)

    AgentClient.invokeHttpRequest[Done](agentRequest) map { result =>
      logger.debug("Agent ARL execution invocation successful on URI:{} for model_id: {} & model_name: {}",
        agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)
      result
    }
  }


    /**
     * This function invokes the ARL execution endpoint of each Agent corresponding to the selected
     * Agents (DatasetSources) of the Dataset.
     *
     * The call on the Agents are in parallel for the given dataMiningModel.
     *
     * @param dataMiningModel
     * @return
     */
    def invokeAgentsARLExecution(dataMiningModel: DataMiningModel): Future[Done] = {
      // Get all Agents of this DataMiningModel to which ARL execution requests will be POSTed
      val agents = DataMiningModelController.getSelectedAgents(dataMiningModel)

      logger.debug("I will invoke the ARL execution endpoints of {} agents with agent-ids: {} for {} number of BoostedModels " +
        "where the Algorithms are {}",
        agents.length, agents.map(_.agent_id).mkString(","), dataMiningModel.boosted_models.get.length, dataMiningModel.boosted_models.get.map(_.algorithm.name).mkString(","))

      Future.sequence(agents.map(invokeARLExecution(_, dataMiningModel))) map { responses =>
        val failedAgents = responses.collect { case Failure(x) => x }
        if (failedAgents.nonEmpty) {
          val msg = s"There are ${failedAgents.size} Agents out of ${responses.size} which returned error on ARL execution request."
          logger.error(msg)
          throw AgentCommunicationException(reason = msg)
        }

        Done
      }
    }

    /**
     * Asks the ARLExecutionResults from the given Agent for the given dataMiningModel.
     *
     * @param agent
     * @param dataMiningModel
     * @return An Option[ARLExecutionResults]. If the result is None, that means the ARL execution has not completed yet.
     */
    private def getARLExecutionResultFromAgent(agent: Agent, dataMiningModel: DataMiningModel): Future[Option[ARLExecutionResult]] = {
      val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.GET, agent.getARLExecutionURI(dataMiningModel.model_id))

      logger.debug("Asking the ARLExecutionResult to the Agent with id:{} on URI:{} for model_id: {} & model_name: {}",
        agent.agent_id, agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)

      AgentClient.invokeHttpRequest[ARLExecutionResult](agentRequest).map(_.toOption)
    }

    /**
     * Asks the ARL execution results of the DataMiningModel to the Agents. These Agents were previously POSTed to
     * start executing ARL algorithms on their datasets.
     * And only the Agents whose results were not received yet are POSTed.
     *
     * @param dataMiningModel
     * @return Returns a sequence of ARLExecutionResult. Only the results of Agents which finished their ARL execution will be returned by this function.
     */
    def askAgentsARLExecutionResults(dataMiningModel: DataMiningModel): Future[Seq[ARLExecutionResult]] = {
      // Get the Agents whose ARLExecutionResult have not been received yet
      val agents = DataMiningModelController.getAgentsWaitedForARLExecutionResults(dataMiningModel)

      logger.debug("I will ask the ARL execution results to {} agents with agent-ids: {}",
        agents.length, agents.map(_.agent_id).mkString(","))

      Future.sequence(agents.map(getARLExecutionResultFromAgent(_, dataMiningModel))) map { responses =>
        responses
          .filter(_.isDefined) // keep if it is ready
          .map(_.get) // get rid of Option
      }
    }

    /**
     * Deletes the ARLExecutionResult of the DataMiningModel indicated by the model_id on the given Agent
     *
     * @param agent
     * @param dataMiningModel
     * @return
     */
    private def deleteARLExecutionResultFromAgent(agent: Agent, dataMiningModel: DataMiningModel): Future[Option[Done]] = {
      val agentRequest = AgentClient.createHttpRequest(agent, HttpMethods.DELETE, agent.getARLExecutionURI(dataMiningModel.model_id))

      logger.debug("Deleting the ARLExecutionResult on the Agent with agent_id:{} on URI:{} for model_id: {} & model_name: {}",
        agent.agent_id, agentRequest.httpRequest.getUri(), dataMiningModel.model_id.get, dataMiningModel.name)

      AgentClient.invokeHttpRequest[Done](agentRequest).map(_.toOption)
    }

    /**
     * Sends DELETE requests for the ARL executions to all selected Agents of the given DataMiningModel.
     *
     * @param dataMiningModel
     * @return
     */
    def deleteAgentsARLExecutionResults(dataMiningModel: DataMiningModel): Future[Done] = {
      val agents = DataMiningModelController.getSelectedAgents(dataMiningModel)

      logger.debug("I will invoke the DELETE ARL execution endpoints of {} agents with agent-ids: {}",
        agents.length, agents.map(_.agent_id).mkString(","))

      Future.sequence(agents.map(deleteARLExecutionResultFromAgent(_, dataMiningModel))) map { responses =>
        if(responses.exists(_.isEmpty)) {
          logger.warn("We are trying to delete the ARL execution results on Agents, however; there is an Agent whose results were not there!")
        }
        Done
      }
    }

}
