package ppddm.core.fhir

import org.json4s.{JArray, JInt, JObject}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

abstract class FHIRQuery(fhirPath: Option[String]) extends Serializable {

  protected def constructQueryString(): String

  def getFHIRPath(): Option[String] = {
    this.fhirPath
  }

  def getCountQuery(): FHIRQuery

  def getPageQuery(count: Int, pageIndex: Int): FHIRQuery

  /**
   * Get the count of the results for the query
   *
   * @param client
   * @return
   */
  def getCount(client: FHIRClient): Future[Int] = {
    getCountQuery().execute(client).map { res =>
      // TODO: What happens if an exception occurs
      (res \ "total").asInstanceOf[JInt].num.toInt
    }
  }

  def getResources(client: FHIRClient, count: Int, pageIndex: Int): Future[Seq[JObject]] = {
    getPageQuery(count, pageIndex).getResources(client)
  }

  def getResources(client: FHIRClient): Future[Seq[JObject]] = {
    // TODO: This query only retrieves the 1st page (default 20).
    // TODO: All resources should be returned by this function.
    execute(client) map { bundle =>
      (bundle \ "entry").asInstanceOf[JArray].arr.map { entry =>
        (entry \ "resource").asInstanceOf[JObject]
      }
    }
  }

  def execute(client: FHIRClient): Future[JObject] = {
    client.searchByUrl(constructQueryString())
  }

}
