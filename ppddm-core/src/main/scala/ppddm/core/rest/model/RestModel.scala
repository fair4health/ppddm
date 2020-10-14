package ppddm.core.rest.model

import java.time.LocalDateTime
import java.util.UUID

import ppddm.core.rest.model.AlgorithmName.AlgorithmName
import ppddm.core.rest.model.DataMiningState.DataMiningState
import ppddm.core.rest.model.SelectionStatus.SelectionStatus
import ppddm.core.rest.model.DataType.DataType
import ppddm.core.rest.model.ExecutionState.ExecutionState
import ppddm.core.rest.model.ProjectType.ProjectType
import ppddm.core.rest.model.VariableDataType.VariableDataType
import ppddm.core.rest.model.VariableType.VariableType
import ppddm.core.util.URLUtil

sealed class ModelClass

case class Project(project_id: Option[String],
                   name: String,
                   description: String,
                   project_type: ProjectType,
                   created_by: String, // The user ID who creates this Project
                   created_on: Option[LocalDateTime]) extends ModelClass {

  def withUniqueProjectId: Project = {
    this.copy(project_id = Some(UUID.randomUUID().toString), created_on = Some(LocalDateTime.now()))
  }
}

case class Featureset(featureset_id: Option[String],
                      project_id: String,
                      name: String,
                      description: String,
                      variables: Option[Seq[Variable]],
                      created_by: String,
                      created_on: Option[LocalDateTime]) extends ModelClass {

  def withUniqueFeaturesetId: Featureset = {
    this.copy(featureset_id = Some(UUID.randomUUID().toString), created_on = Some(LocalDateTime.now()))
  }
}

case class Variable(name: String,
                    description: Option[String],
                    fhir_query: String,
                    fhir_path: String,
                    variable_data_type: VariableDataType,
                    variable_type: VariableType) extends ModelClass

final case class Dataset(dataset_id: Option[String],
                         project_id: String,
                         featureset: Featureset,
                         name: String,
                         description: String,
                         eligibility_criteria: Seq[EligibilityCriterion],
                         dataset_sources: Option[Seq[DatasetSource]],
                         execution_state: Option[ExecutionState],
                         created_by: String,
                         created_on: Option[LocalDateTime]) extends ModelClass {

  def withUniqueDatasetId: Dataset = {
    this.copy(dataset_id = Some(UUID.randomUUID().toString), created_on = Some(LocalDateTime.now()))
  }

  def withDatasetSources(dataset_sources: Seq[DatasetSource]): Dataset = {
    val newDataset = this.copy(dataset_sources = Some(dataset_sources))
    withUpdatedExecutionState(newDataset)
  }

  def withUpdatedExecutionState(dataset: Dataset = this): Dataset = {
    if (dataset.dataset_sources.isEmpty) {
      // Do not do anything
      dataset
    } else {
      val selectedDataSources = dataset.dataset_sources.get.filter(s => s.selection_status.isDefined && s.selection_status.get == SelectionStatus.SELECTED)
      if (selectedDataSources.nonEmpty) {
        // This means there are selected data sources, the state should be FINAL
        if (!dataset.execution_state.contains(ExecutionState.FINAL)) {
          dataset.copy(execution_state = Some(ExecutionState.FINAL))
        } else {
          // Do nothing if it is already in FINAL state
          dataset
        }
      } else {
        // Find the ExecutionState for the newly created Dataset
        val areAllAgentsFinished = dataset.dataset_sources.get
          // Set it to True if the execution_state is defined and it is recieved as FINAL from the Agent, False otherwise
          .map(s => s.execution_state.isDefined && s.execution_state.get == ExecutionState.FINAL)
          .reduceLeft((a, b) => a && b) // Logically AND the states. If all sources are True, then Dataset's state can become READY
        val newExecutionState = if (areAllAgentsFinished) Some(ExecutionState.READY) else Some(ExecutionState.EXECUTING)
        dataset.copy(execution_state = newExecutionState)
      }
    }
  }
}

final case class EligibilityCriterion(fhir_query: String,
                                      fhir_path: Option[String]) extends ModelClass

final case class DatasetSource(agent: Agent,
                               agent_data_statistics: Option[AgentDataStatistics],
                               selection_status: Option[SelectionStatus],
                               execution_state: Option[ExecutionState]) extends ModelClass

final case class Agent(agent_id: String,
                       name: String,
                       endpoint: String) extends ModelClass {

  private def getURI(path: String, id: Option[String] = None): String = {
    if (id.isDefined) {
      URLUtil.append(endpoint, path, id.get)
    } else {
      URLUtil.append(endpoint, path)
    }
  }

  def getDataPreparationURI(dataset_id: Option[String] = None): String = {
    getURI("prepare", dataset_id)
  }

  def getTrainingURI(model_id: Option[String] = None): String = {
    getURI("dm/train", model_id)
  }

  def getValidationURI(model_id: Option[String] = None): String = {
    getURI("dm/validate", model_id)
  }

  def getTestURI(model_id: Option[String] = None): String = {
    getURI("dm/test", model_id)
  }
}

final case class AgentDataStatistics(number_of_records: Long,
                                     variable_statistics: Seq[VariableStatistics]) extends ModelClass

final case class VariableStatistics(variable: Variable,
                                    min_value: Option[Double],
                                    max_value: Option[Double],
                                    null_percentage: Option[Double]) extends ModelClass

final case class DataPreparationRequest(dataset_id: String,
                                        agent: Agent,
                                        featureset: Featureset,
                                        eligibility_criteria: Seq[EligibilityCriterion],
                                        submitted_by: String) extends ModelClass

final case class DataPreparationResult(dataset_id: String,
                                       agent: Agent,
                                       agent_data_statistics: AgentDataStatistics) extends ModelClass

final case class Parameter(name: String,
                           data_type: DataType,
                           value: String) extends ModelClass {

  def getValueAsDoubleArray(): Array[Double] = {
    if (value.contains(",")) { // It is provided as Array
      value.split(",").map(_.toDouble)
    } else {
      Array(value.toDouble)
    }
  }

  def getValueAsIntArray(): Array[Int] = {
    if (value.contains(",")) { // It is provided as Array
      value.split(",").map(_.toInt)
    } else {
      Array(value.toInt)
    }
  }
}

object Parameter {
  def apply(name: String, data_type: DataType, value: Double): Parameter = {
    Parameter(name, data_type, value.toString)
  }

  def apply(name: String, data_type: DataType, value: Int): Parameter = {
    Parameter(name, data_type, value.toString)
  }
}

final case class DataMiningModel(model_id: Option[String],
                                 project_id: String,
                                 dataset: Dataset,
                                 name: String,
                                 description: String,
                                 algorithms: Seq[Algorithm],
                                 algorithm_models: Option[Seq[BoostedModel]],
                                 data_mining_state: Option[DataMiningState],
                                 created_by: String,
                                 created_on: Option[LocalDateTime]) extends ModelClass {

  def withUniqueModelId: DataMiningModel = {
    this.copy(model_id = Some(UUID.randomUUID().toString), created_on = Some(LocalDateTime.now()))
  }

  def withDataMiningState(dataMiningState: DataMiningState): DataMiningModel = {
    this.copy(data_mining_state = Some(dataMiningState))
  }
}

final case class BoostedModel(algorithm: Algorithm,
                              weak_models: Seq[WeakModel],
                              training_statistics: Seq[Parameter], // Will be calculated bu using the calculated_training_statistics and weight of each WeakModel
                              test_statistics: Option[Seq[Parameter]],
                              data_mining_state: DataMiningState) extends ModelClass

final case class WeakModel(algorithm: Algorithm,
                           agent: Agent,
                           fitted_model: String,
                           training_statistics: Seq[AgentAlgorithmStatistics], // Includes its Agent's training statistics + other Agents' validation statistics
                           calculated_training_statistics: Option[Seq[Parameter]], // Will be calculated after training and validation statistics are received (together with the weight of this WeakModel)
                           weight: Option[Double],
                           data_mining_state: Option[DataMiningState]) extends ModelClass

final case class Algorithm(name: AlgorithmName,
                           parameters: Seq[Parameter]) extends ModelClass

final case class AgentAlgorithmStatistics(agent_model: Agent,
                                          agent_statistics: Agent,
                                          algorithm: Algorithm,
                                          statistics: Seq[Parameter]) extends ModelClass

final case class ModelTrainingRequest(model_id: String,
                                      dataset_id: String,
                                      agent: Agent,
                                      algorithms: Seq[Algorithm],
                                      submitted_by: String) extends ModelClass

final case class ModelTrainingResult(model_id: String,
                                     dataset_id: String,
                                     agent: Agent,
                                     algorithm_training_models: Seq[WeakModel]) extends ModelClass

final case class ModelValidationRequest(model_id: String,
                                        dataset_id: String,
                                        agent: Agent,
                                        weak_models: Seq[WeakModel],
                                        submitted_by: String) extends ModelClass

final case class ModelValidationResult(model_id: String,
                                       dataset_id: String,
                                       agent: Agent,
                                       validation_statistics: Seq[AgentAlgorithmStatistics]) extends ModelClass

final case class ModelTestRequest(model_id: String,
                                  dataset_id: String,
                                  agent: Agent,
                                  boosted_models: Seq[BoostedModel]) extends ModelClass

final case class ModelTestResult(model_id: String,
                                 dataset_id: String,
                                 agent: Agent,
                                 test_statistics: Seq[AgentAlgorithmStatistics]) extends ModelClass
