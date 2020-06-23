package ppddm.manager.controller.query

import java.util.UUID

import ppddm.core.rest.model.{DataSource, DataSourceStatus, Dataset}

object FederatedQueryManager {

  def invokeAgents(dataset: Dataset): Seq[DataSource] = {
    // TODO 1. Fetch data sources from Service Registry

    // 2. Create DataSource object for each data source in QUERYING status.
    val dataSources = Seq[DataSource](
      DataSource(Some(UUID.randomUUID().toString), "SAS", DataSourceStatus.QUERYING, "/", None, None),
      DataSource(Some(UUID.randomUUID().toString), "UNIGE", DataSourceStatus.QUERYING, "/", None, None),
      DataSource(Some(UUID.randomUUID().toString), "IACS", DataSourceStatus.QUERYING, "/", None, None),
      DataSource(Some(UUID.randomUUID().toString), "UCSC", DataSourceStatus.QUERYING, "/", None, None),
      DataSource(Some(UUID.randomUUID().toString), "UP", DataSourceStatus.QUERYING, "/", None, None)
    ) // TODO to be deleted

    // TODO 3. Invoke agents to start data extraction process

    dataSources
  }
}
