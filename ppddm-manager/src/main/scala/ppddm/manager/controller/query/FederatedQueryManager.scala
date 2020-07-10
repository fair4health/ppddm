package ppddm.manager.controller.query

import java.util.UUID

import ppddm.core.rest.model.{DataPreparationRequest, DataSource, Dataset, DatasetSource, ExecutionState}
import ppddm.core.util.JsonFormatter

object FederatedQueryManager {

  def invokeAgents(dataset: Dataset): Seq[DatasetSource] = {
    // TODO 1. Fetch data sources from Service Registry

    // 2. Create DataSource object for each data source in IN_PROGRESS status.
    val datasetSources = Seq[DatasetSource](
      DatasetSource(DataSource(UUID.randomUUID().toString, "SAS", "sas.com/agent"), None, None, Some(ExecutionState.QUERYING)),
      DatasetSource(DataSource(UUID.randomUUID().toString, "UNIGE", "unige.com/agent"), None, None, Some(ExecutionState.QUERYING)),
      DatasetSource(DataSource(UUID.randomUUID().toString, "IACS", "iacs.com/agent"), None, None, Some(ExecutionState.QUERYING)),
      DatasetSource(DataSource(UUID.randomUUID().toString, "UCSC", "ucsc.com/agent"), None, None, Some(ExecutionState.QUERYING)),
      DatasetSource(DataSource(UUID.randomUUID().toString, "UP", "up.com/agent"), None, None, Some(ExecutionState.QUERYING))
    ) // TODO to be deleted

    // TODO 3. Invoke agents to start data extraction process
    val dataPreperationRequest = DataPreparationRequest(dataset.dataset_id.get, dataset.featureset, dataset.eligibility_criteria, "1903") // TODO submitted by
    println(JsonFormatter.convertToJson(dataPreperationRequest).toPrettyJson)

    datasetSources
  }
}
