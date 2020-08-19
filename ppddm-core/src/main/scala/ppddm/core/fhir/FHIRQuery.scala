package ppddm.core.fhir

import org.json4s.{JArray, JInt, JObject}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait FHIRQuery extends Serializable {

  protected def constructQueryString(): String

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
