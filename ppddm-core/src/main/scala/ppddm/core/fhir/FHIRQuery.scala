package ppddm.core.fhir

import ca.uhn.fhir.rest.client.api.IGenericClient
import org.hl7.fhir.r4.model.{Bundle, DomainResource}

import collection.JavaConverters._

trait FHIRQuery extends Serializable {

  protected def constructQueryString(fhirServerBaseUri: String): String

  def getCountQuery(): FHIRQuery

  def getPageQuery(count: Int, pageIndex: Int): FHIRQuery

  /**
   * Get the count of the results for the query
   *
   * @param client
   * @return
   */
  def getCount(client: IGenericClient): Int = {
    getCountQuery().execute(client).getTotal()
  }

  /**
   * Get the resources for the query
   *
   * @param client
   * @param count
   * @param pageIndex
   * @tparam T
   * @return
   */
  def getResources[T <: DomainResource](client: IGenericClient, count: Int, pageIndex: Int): Seq[T] = {
    getPageQuery(count, pageIndex)
      .execute(client)
      .getEntry.asScala.map(_.getResource.asInstanceOf[T])
  }

  def getResources[T <: DomainResource](client: IGenericClient): Seq[T] = {
    execute(client)
      .getEntry.asScala.map(_.getResource.asInstanceOf[T])
  }

  def execute(client: IGenericClient): Bundle = {
    client.search().byUrl(constructQueryString(client.getServerBase)).execute()
  }

}
