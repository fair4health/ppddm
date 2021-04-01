package ppddm.core.rest.model

import ppddm.core.rest.model.AlgorithmName.AlgorithmName
import ppddm.core.rest.model.CategoricalEncodingType.CategoricalEncodingType
import ppddm.core.rest.model.DataMiningState.DataMiningState
import ppddm.core.rest.model.DataType.DataType
import ppddm.core.rest.model.ExecutionState.ExecutionState
import ppddm.core.rest.model.MissingDataOperationType.MissingDataOperationType
import ppddm.core.rest.model.ProjectType.ProjectType
import ppddm.core.rest.model.SelectionStatus.SelectionStatus
import ppddm.core.rest.model.Variable.removeInvalidChars
import ppddm.core.rest.model.VariableDataType.VariableDataType
import ppddm.core.rest.model.VariableType.VariableType
import ppddm.core.util.{JsonClass, URLUtil}

import java.time.LocalDateTime
import java.util.UUID

sealed class ModelClass extends JsonClass

case class Project(project_id: Option[String],
                   name: String,
                   description: String,
                   project_type: ProjectType,
                   created_by: String, // The user ID who creates this Project
                   created_on: Option[LocalDateTime],
                   updated_on: Option[LocalDateTime]) extends ModelClass {

  def withUniqueProjectId: Project = {
    val now = LocalDateTime.now()
    this.copy(project_id = Some(UUID.randomUUID().toString), created_on = Some(now), updated_on = Some(now))
  }

  def withLastUpdated: Project = {
    this.copy(updated_on = Some(LocalDateTime.now()))
  }
}

final case class Featureset(featureset_id: Option[String],
                            project_id: String,
                            name: String,
                            description: String,
                            variables: Seq[Variable],
                            created_by: String,
                            created_on: Option[LocalDateTime],
                            updated_on: Option[LocalDateTime]) extends ModelClass {

  def withUniqueFeaturesetId: Featureset = {
    val now = LocalDateTime.now()
    this.copy(featureset_id = Some(UUID.randomUUID().toString), created_on = Some(now), updated_on = Some(now))
  }

  def withLastUpdated: Featureset = {
    this.copy(updated_on = Some(LocalDateTime.now()))
  }

  def withNewProjectId(newProjectId: String): Featureset = {
    this.copy(project_id = newProjectId)
  }
}

final case class Variable(name: String,
                          description: Option[String],
                          fhir_query: String,
                          fhir_path: String,
                          variable_data_type: VariableDataType,
                          variable_type: VariableType) extends ModelClass {
  def getMLValidName: String = {
    removeInvalidChars(name)
  }
}

object Variable {
  /**
   * Removes invalid characters that prevent the recording and filtering of the Parquet files from being done correctly.
   *
   * @param value
   * @return
   */
  def removeInvalidChars(value: String): String = {
    value.trim.replaceAll("[\\s\\`\\*{}\\[\\]()>#\\+:\\~'%\\^&@<\\?;,\\\"!\\$=\\|\\.]", "")
  }
}

final case class Dataset(dataset_id: Option[String],
                         project_id: String,
                         featureset: Featureset,
                         name: String,
                         description: String,
                         eligibility_criteria: Seq[EligibilityCriterion],
                         dataset_sources: Option[Seq[DatasetSource]],
                         execution_state: Option[ExecutionState],
                         created_by: String,
                         created_on: Option[LocalDateTime],
                         updated_on: Option[LocalDateTime]) extends ModelClass {

  def withUniqueDatasetId: Dataset = {
    val now = LocalDateTime.now()
    this.copy(dataset_id = Some(UUID.randomUUID().toString), created_on = Some(now), updated_on = Some(now))
  }

  def withLastUpdated: Dataset = {
    this.copy(updated_on = Some(LocalDateTime.now()))
  }

  def withNewProjectId(newProjectId: String): Dataset = {
    this.copy(project_id = newProjectId)
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
      // First, check if there are any Erroneous DatasetSources.
      // If there is, then set this Dataset's ExecutionState to ERROR
      val erroneousDataSources = dataset.dataset_sources.get.filter(s => s.execution_state.isDefined && s.execution_state.get == ExecutionState.ERROR)
      if (erroneousDataSources.nonEmpty) {
        if (!dataset.execution_state.contains(ExecutionState.ERROR)) {
          dataset.copy(execution_state = Some(ExecutionState.ERROR))
        } else {
          // Do nothing if it is already in ERROR state
          dataset
        }
      } else {
        // There is not error on the DatasetSources, check for other state changes. Start with the selection status.
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
}

final case class EligibilityCriterion(fhir_query: String,
                                      fhir_path: Option[String]) extends ModelClass

final case class DatasetSource(agent: Agent,
                               agent_data_statistics: Option[AgentDataStatistics],
                               selection_status: Option[SelectionStatus],
                               execution_state: Option[ExecutionState],
                               error_message: Option[String] = None) extends ModelClass

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
    getURI("dm/classification/train", model_id)
  }

  def getValidationURI(model_id: Option[String] = None): String = {
    getURI("dm/classification/validate", model_id)
  }

  def getTestURI(model_id: Option[String] = None): String = {
    getURI("dm/classification/test", model_id)
  }

  def getARLFrequencyCalculationURI(model_id: Option[String] = None): String = {
    getURI("dm/arl/frequency", model_id)
  }

  def getARLExecutionURI(model_id: Option[String] = None): String = {
    getURI("dm/arl/execute", model_id)
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
                                       agent_data_statistics: AgentDataStatistics,
                                       exception: Option[String]) extends ModelClass

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

  def getValueAsStringArray(): Array[String] = {
    if (value.contains(",")) { // It is provided as Array
      value.split(",")
    } else {
      Array(value)
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
                                 variable_configurations: Option[Seq[VariableConfiguration]], // Prediction
                                 algorithms: Seq[Algorithm],
                                 training_size: Option[Double], // Prediction
                                 test_size: Option[Double], // Prediction
                                 boosted_models: Option[Seq[BoostedModel]],
                                 data_mining_state: Option[DataMiningState],
                                 error_message: Option[String] = None,
                                 created_by: String,
                                 created_on: Option[LocalDateTime],
                                 updated_on: Option[LocalDateTime]) extends ModelClass {

  def withUniqueModelId: DataMiningModel = {
    val now = LocalDateTime.now()
    this.copy(model_id = Some(UUID.randomUUID().toString), created_on = Some(now), updated_on = Some(now))
  }

  def withLastUpdated: DataMiningModel = {
    this.copy(updated_on = Some(LocalDateTime.now()))
  }

  def withNewProjectId(newProjectId: String): DataMiningModel = {
    this.copy(project_id = newProjectId)
  }

  def withDataMiningState(dataMiningState: DataMiningState): DataMiningModel = {
    this.copy(data_mining_state = Some(dataMiningState))
  }

  def withBoostedModels(boostedModels: Seq[BoostedModel]): DataMiningModel = {
    this.copy(boosted_models = Some(boostedModels))
  }

  def withError(errorMessage: String): DataMiningModel = {
    this.copy(error_message = Some(errorMessage), data_mining_state = Some(DataMiningState.ERROR))
  }

  def withUpdatedDataMiningState(dataMiningModel: DataMiningModel = this): DataMiningModel = {
    if (dataMiningModel.boosted_models.isEmpty) {
      // Do not do anything
      dataMiningModel
    } else {
      val selectedDataMiningModels = dataMiningModel.boosted_models.get.filter(s => s.selection_status.isDefined && s.selection_status.get == SelectionStatus.SELECTED)
      if (selectedDataMiningModels.nonEmpty) {
        // This means there are selected data mining models, the state should be FINAL
        if (!dataMiningModel.data_mining_state.contains(DataMiningState.FINAL)) {
          dataMiningModel.copy(data_mining_state = Some(DataMiningState.FINAL))
        } else {
          // Do nothing if it is already in FINAL state
          dataMiningModel
        }
      } else {
        // Do nothing if no BoostedModel is selected
        dataMiningModel
      }
    }
  }

  def getSelectedBoostedModel(dataMiningModel: DataMiningModel = this): BoostedModel = {
    dataMiningModel.boosted_models.get.filter(boosted_model => boosted_model.selection_status.get == SelectionStatus.SELECTED).head
  }
}

final case class VariableConfiguration(variable: Variable,
                                       encoding_type: Option[CategoricalEncodingType],
                                       missing_data_operation: Option[MissingDataOperationType],
                                       missing_data_specific_value: Option[Double]) extends ModelClass

final case class BoostedModel(algorithm: Algorithm,
                              weak_models: Seq[WeakModel],
                              combined_frequent_items: Option[Seq[Parameter]], // Association
                              combined_total_record_count: Option[Long], // Association
                              combined_association_rules: Option[Seq[AssociationRule]], // Association
                              test_statistics: Option[Seq[AgentAlgorithmStatistics]], // Prediction
                              calculated_test_statistics: Option[Seq[Parameter]], // Prediction
                              selection_status: Option[SelectionStatus]) extends ModelClass {

  def replaceWeakModels(weakModels: Seq[WeakModel]): BoostedModel = {
    val existingWeakModels = this.weak_models.map(wm => (wm.algorithm.name, wm.agent.agent_id)).toSet
    val newWeakModels = weakModels.map(wm => (wm.algorithm.name, wm.agent.agent_id)).toSet
    if (!existingWeakModels.equals(newWeakModels)) {
      val msg = s"You are trying to replace the whole WeakModels of this BoostedModel for Algorithm:${this.algorithm}, but they do not MATCH!! " +
        s"You can only replace the WeakModels if you already pass the new WeakModels for all existing WeakModels with respect to Algorithm and Agent."
      throw new IllegalArgumentException(msg)
    }

    this.copy(weak_models = weakModels)
  }

  def addNewWeakModels(weakModels: Seq[WeakModel]): BoostedModel = {
    // Let's perform some integrity checks

    val illegalAlgorithm = weakModels.find(_.algorithm.name != algorithm.name)
    if (illegalAlgorithm.nonEmpty) {
      // Ooops! We have a problem  Houston.
      val msg = s"Given Algorithm with name ${illegalAlgorithm.get.algorithm.name} cannot be added to this BoostedModel " +
        s"because this BoostedModel is for Algorithm with name ${this.algorithm.name}"
      throw new IllegalArgumentException(msg)
    }

    val existingAgents = this.weak_models.map(_.agent).toSet
    val newAgents = weakModels.map(_.agent).toSet
    val illegalAgents = existingAgents.intersect(newAgents)
    if (illegalAgents.nonEmpty) {
      // We have another problem Houston!
      val msg = s"You send new WeakModels, but they already exist in this BoostedModel of algorithm:${this.algorithm.name}. " +
        s"Agent names are ${illegalAgents.map(_.name).mkString(",")}"
      throw new IllegalArgumentException(msg)
    }

    // We are good to go!
    this.copy(weak_models = this.weak_models ++ weakModels)
  }

  def addNewTestStatistics(testStatistics: Seq[AgentAlgorithmStatistics]): BoostedModel = {
    val existingAgents = this.test_statistics.getOrElse(Seq.empty).map(_.agent_statistics)
    val newAgents = testStatistics.map(_.agent_statistics)
    val illegalAgents = existingAgents.intersect(newAgents)
    if (illegalAgents.nonEmpty) {
      val msg = s"You send new AgentAlgorithmResults from some Agents, but they already exist in this BoostedModel of algorithm:${this.algorithm.name}. " +
        s"Agent names are ${illegalAgents.map(_.name).mkString(",")}"
      throw new IllegalArgumentException(msg)
    }

    // We are good to go!
    val updatedStatistics = if (this.test_statistics.isEmpty) Some(testStatistics) else Some(this.test_statistics.get ++ testStatistics)
    this.copy(test_statistics = updatedStatistics)
  }

  def withCalculatedTestStatistics(calculated_test_statistics: Seq[Parameter]): BoostedModel = {
    this.copy(calculated_test_statistics = Some(calculated_test_statistics))
  }

  def withCombinedFrequentItems(combined_frequent_items: Seq[Parameter]): BoostedModel = {
    this.copy(combined_frequent_items = Some(combined_frequent_items))
  }

  def withCombinedTotalRecordCount(combined_total_record_count: Long): BoostedModel = {
    this.copy(combined_total_record_count = Some(combined_total_record_count))
  }

  def withCombinedAssociationRules(combined_association_rules: Seq[AssociationRule]): BoostedModel = {
    this.copy(combined_association_rules = Some(combined_association_rules))
  }

}

final case class WeakModel(algorithm: Algorithm,
                           agent: Agent,
                           fitted_model: Option[String], // Prediction
                           training_statistics: Option[AgentAlgorithmStatistics], // Prediction // Includes its Agent's training statistics
                           validation_statistics: Option[Seq[AgentAlgorithmStatistics]], // Prediction // Includes other Agents' validation statistics // Prediction
                           calculated_statistics: Option[Seq[Parameter]], // Prediction // Will be calculated after training and validation statistics are received (together with the weight of this WeakModel)
                           weight: Option[Double], // Prediction
                           item_frequencies: Option[Seq[Parameter]], // Association // Frequency of items in the agent. Generated in the 1st step, that is frequency calculation
                           total_record_count: Option[Long], // Association // Total number of records in the agent. Generated in the 1st step, that is frequency calculation
                           frequent_itemsets: Option[Seq[FrequentItemset]], // Association // Frequency of itemsets that are above the min support (threshold). Generated in the 2nd step, that is ARL model execution
                           association_rules: Option[Seq[AssociationRule]] // Association // Association rules with confidence and lift. Generated in the 2nd step, that is ARL model execution
                          ) extends ModelClass {

  def withFittedModel(fitted_model: String): WeakModel = {
    if (this.fitted_model.isDefined) {
      val msg = "You are trying to replace an already existing fitted_model in this WeakModel. This is not allowed."
      throw new IllegalArgumentException(msg)
    }
    if (this.item_frequencies.isEmpty) {
      val msg = "While assigning a fitted_model to a WeakModel, the WeakModel must have the item_frequencies (considering the ARL mining)"
      throw new IllegalArgumentException(msg)
    }
    this.copy(fitted_model = Some(fitted_model))
  }

  def addNewValidationStatistics(validationStatistics: Seq[AgentAlgorithmStatistics]): WeakModel = {
    // Let's perform some integrity checks

    val illegalAgent = validationStatistics.find(_.agent_model.agent_id != this.agent.agent_id)
    if (illegalAgent.isDefined) {
      val msg = s"You are trying to add validation statistics to the WeakModel trained on Agent:${this.agent.agent_id}, " +
        s"but the received validation statistics say that its model was trained on Agent:${illegalAgent.get.agent_model}. " +
        s"The statistics are calculated on Agent:${illegalAgent.get.agent_statistics}."
      throw new IllegalArgumentException(msg)
    }

    val illegalValidation = validationStatistics.find(_.agent_statistics.agent_id == this.agent.agent_id)
    if (illegalValidation.isDefined) {
      val msg = s"You are trying to add validation statistics to the WeakModel trained on Agent:${this.agent}, " +
        s"but the received validation statistics say that the validation is also performed on the same Agent, " +
        s"which is IMPOSSIBLE in a correctly distributed ML execution."
      throw new IllegalArgumentException(msg)
    }

    val illegalStatistics = this.validation_statistics.getOrElse(Seq.empty[AgentAlgorithmStatistics])
      .map(s => (s.agent_model.agent_id, s.agent_statistics.agent_id, s.algorithm.name)) // Convert to (Agent, Agent, Algorithm) to check the equality without the statistics
      .toSet
      .intersect(
        validationStatistics.map(s => (s.agent_model.agent_id, s.agent_statistics.agent_id, s.algorithm.name)).toSet)
    if (illegalStatistics.nonEmpty) {
      val msg = s"You give me new validation statistics, but they already exist within this WeakModel with Algorithm:${this.algorithm.name} " +
        s"and Agent:${this.agent.name}."
      throw new IllegalArgumentException(msg)
    }

    // We are good to go!
    this.copy(validation_statistics = Some(this.validation_statistics.getOrElse(Seq.empty[AgentAlgorithmStatistics]) ++ validationStatistics))
  }

  def withCalculatedStatistics(calculated_statistics: Seq[Parameter]): WeakModel = {
    this.copy(calculated_statistics = Some(calculated_statistics))
  }

  def withWeight(weight: Double): WeakModel = {
    this.copy(weight = Some(weight))
  }

  def withFreqItemsetAndAssociationRules(frequent_itemsets: Seq[FrequentItemset], association_rules: Seq[AssociationRule]): WeakModel = {
    this.copy(frequent_itemsets = Some(frequent_itemsets), association_rules = Some(association_rules))
  }

}

final case class Algorithm(name: AlgorithmName,
                           display: Option[String],
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
                                     algorithm_training_models: Seq[WeakModel],
                                     exception: Option[String]) extends ModelClass

final case class ModelValidationRequest(model_id: String,
                                        dataset_id: String,
                                        agent: Agent,
                                        weak_models: Seq[WeakModel],
                                        submitted_by: String) extends ModelClass

final case class ModelValidationResult(model_id: String,
                                       dataset_id: String,
                                       agent: Agent,
                                       validation_statistics: Seq[AgentAlgorithmStatistics],
                                       exception: Option[String]) extends ModelClass

final case class ModelTestRequest(model_id: String,
                                  dataset_id: String,
                                  agent: Agent,
                                  boosted_models: Seq[BoostedModel],
                                  submitted_by: String) extends ModelClass

final case class ModelTestResult(model_id: String,
                                 dataset_id: String,
                                 agent: Agent,
                                 test_statistics: Seq[AgentAlgorithmStatistics],
                                 exception: Option[String]) extends ModelClass

final case class ProspectiveStudy(prospective_study_id: Option[String],
                                  project_id: String,
                                  name: String,
                                  description: String,
                                  data_mining_model: DataMiningModel,
                                  predictions: Seq[PredictionResult],
                                  created_by: String,
                                  created_on: Option[LocalDateTime],
                                  updated_on: Option[LocalDateTime]) extends ModelClass {

  def withUniqueProspectiveStudyId: ProspectiveStudy = {
    val now = LocalDateTime.now()
    this.copy(prospective_study_id = Some(UUID.randomUUID().toString), created_on = Some(now), updated_on = Some(now))
  }

  def withLastUpdated: ProspectiveStudy = {
    this.copy(updated_on = Some(LocalDateTime.now()))
  }

  def withNewProjectId(newProjectId: String): ProspectiveStudy = {
    this.copy(project_id = newProjectId)
  }
}

final case class PredictionRequest(data_mining_model: DataMiningModel,
                                   identifier: String,
                                   variables: Seq[Parameter],
                                   submitted_by: String) extends ModelClass

final case class PredictionResult(identifier: String,
                                  variables: Seq[Parameter],
                                  prediction: Double,
                                  probability: Option[Double]) extends ModelClass

final case class ARLFrequencyCalculationRequest(model_id: String,
                                                dataset_id: String,
                                                agent: Agent,
                                                submitted_by: String) extends ModelClass

final case class ARLFrequencyCalculationResult(model_id: String,
                                               dataset_id: String,
                                               agent: Agent,
                                               item_frequencies: Seq[Parameter],
                                               total_record_count: Long) extends ModelClass

final case class AlgorithmItemSet(algorithm: Algorithm,
                                  items: Seq[String]) extends ModelClass

final case class ARLExecutionRequest(model_id: String,
                                     dataset_id: String,
                                     agent: Agent,
                                     algorithm_set: Seq[AlgorithmItemSet],
                                     submitted_by: String) extends ModelClass

final case class ARLExecutionResult(model_id: String,
                                    dataset_id: String,
                                    agent: Agent,
                                    arl_models: Seq[ARLModel]) extends ModelClass

final case class ARLModel(algorithm: Algorithm,
                          agent: Agent,
                          frequent_itemsets: Seq[FrequentItemset],
                          association_rules: Seq[AssociationRule]) extends ModelClass

final case class FrequentItemset(items: Seq[String],
                                 freq: Long) extends ModelClass

final case class AssociationRule(antecedent: Seq[String],
                                 consequent: Seq[String],
                                 confidence: Double,
                                 lift: Double) extends ModelClass

final case class EncounterBasedItem(subject: String,
                                    periodStart: String,
                                    periodEnd: String) extends ModelClass
