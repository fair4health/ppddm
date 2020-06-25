package ppddm.manager.controller.query

import java.util.UUID

import ppddm.core.rest.model.{DataSource, DataSourceSelectionStatus, Dataset, DatasetSource, ExecutionState}

object FederatedQueryManager {

  def invokeAgents(dataset: Dataset): Seq[DatasetSource] = {
    // TODO 1. Fetch data sources from Service Registry

    // 2. Create DataSource object for each data source in IN_PROGRESS status.
    val datasetSources = Seq[DatasetSource](
      DatasetSource(DataSource(UUID.randomUUID().toString, "SAS", "sas.com/agent"), None, DataSourceSelectionStatus.DISCARDED, ExecutionState.IN_PROGRESS),
      DatasetSource(DataSource(UUID.randomUUID().toString, "UNIGE", "unige.com/agent"), None, DataSourceSelectionStatus.DISCARDED, ExecutionState.IN_PROGRESS),
      DatasetSource(DataSource(UUID.randomUUID().toString, "IACS", "iacs.com/agent"), None, DataSourceSelectionStatus.DISCARDED, ExecutionState.IN_PROGRESS),
      DatasetSource(DataSource(UUID.randomUUID().toString, "UCSC", "ucsc.com/agent"), None, DataSourceSelectionStatus.DISCARDED, ExecutionState.IN_PROGRESS),
      DatasetSource(DataSource(UUID.randomUUID().toString, "UP", "up.com/agent"), None, DataSourceSelectionStatus.DISCARDED, ExecutionState.IN_PROGRESS)
    ) // TODO to be deleted

    // TODO 3. Invoke agents to start data extraction process

    datasetSources
  }
}
