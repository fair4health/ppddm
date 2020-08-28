package ppddm.manager.controller.query

import java.util.UUID

import ppddm.core.rest.model.{DataPreparationRequest, DataSource, Dataset, DatasetSource, ExecutionState}
import ppddm.core.util.JsonFormatter
import ppddm.manager.registry.AgentRegistry

object FederatedQueryManager {

  def invokeAgents(dataset: Dataset): Seq[DatasetSource] = {
    // TODO 1. Fetch data sources from Service Registry

    // 2. Create DataSource objects for each data source with QUERYING status.
    val datasetSources = AgentRegistry.dataSources.map {ds =>
      DatasetSource(ds, None, None, Some(ExecutionState.QUERYING))
    }

    // TODO 3. Invoke agents to start data extraction process
    val dataPreperationRequest = DataPreparationRequest(dataset.dataset_id.get, dataset.featureset, dataset.eligibility_criteria, "1903") // TODO submitted by
    println(JsonFormatter.convertToJson(dataPreperationRequest).toPrettyJson)

    datasetSources
  }
}
