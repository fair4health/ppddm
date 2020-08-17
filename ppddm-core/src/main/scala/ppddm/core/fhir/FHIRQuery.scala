package ppddm.core.fhir

import org.hl7.fhir.r4.model.{Bundle, DomainResource}
import ca.uhn.fhir.rest.client.api.IGenericClient

import collection.JavaConverters._

case class FhirQuery (resourceType:String, params:Seq[(String, String)]) extends Serializable {

  private def constructHAPIQuery():String = {
    var query = s"${resourceType}"

    if(params.nonEmpty)
      query = query + "?" + params.map(p => s"${p._1}=${p._2}").mkString("&")

    query
  }

  private def getCountQuery():FhirQuery = {
    this.copy(resourceType, params :+ ("_summary" -> "count"))
  }

  private def getPageQuery(count:Int, pageIndex:Int):FhirQuery = {
    this.copy(resourceType, params ++ Seq("_count"-> count.toString, "_page" -> pageIndex.toString))
  }

  private def executeQuery(client:IGenericClient):Bundle = {
    client.search().byUrl(constructHAPIQuery()).execute()
  }

  /**
   * Get the count of the results for the query
   * @param client
   * @return
   */
  def getCount(client:IGenericClient):Int = {
    getCountQuery().executeQuery(client).getTotal()
  }

  /**
   * Get the resources for the query
   * @param client
   * @param count
   * @param pageIndex
   * @tparam T
   * @return
   */
  def getResources[T<: DomainResource](client:IGenericClient, count:Int, pageIndex:Int):Seq[T] = {
    getPageQuery(count, pageIndex)
      .executeQuery(client)
      .getEntry.asScala.map(_.getResource.asInstanceOf[T])
  }
}
