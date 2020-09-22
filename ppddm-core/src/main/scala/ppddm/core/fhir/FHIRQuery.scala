package ppddm.core.fhir

import org.json4s.JsonAST.{JString, JValue}
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

  /**
   * Returns the resulting resources for the query
   *
   * @param client Fhir Client
   * @param all Boolean value to understand whether it wants all the resources to be retrieved or the resources on the first page
   * @return
   */
  def getResources(client: FHIRClient, all: Boolean = false): Future[Seq[JObject]] = {
    if (all) {
      getAllResources(client, Seq.empty[JObject], 1)
    } else {
      execute(client) map { bundle =>
        (bundle \ "entry").asInstanceOf[JArray].arr.map { entry =>
          (entry \ "resource").asInstanceOf[JObject]
        }
      }
    }
  }

  /**
   * Returns all the resources recursively
   *
   * @param client Fhir Client
   * @param resources Seq of JObjects - provide Seq.empty[JObject] as initial value
   * @param pageIndex Page index - provide 1 as initial value
   * @return
   */
  private def getAllResources(client: FHIRClient, resources: Seq[JObject], pageIndex: Int): Future[Seq[JObject]] = {
    var currResources: Seq[JObject] = resources
    // Create query with page size 20 and given page index
    getPageQuery(20, pageIndex).execute(client) flatMap { bundle =>
      // Get the bundle.link array
      val bundleLink = (bundle \ "link").asInstanceOf[JArray].arr
      // Search for 'next' relation in the bundle.link object to detect whether the query response has next page
      val hasNext: Option[JValue] = bundleLink.find(item => (item \ "relation").asInstanceOf[JString].s.equals("next"))
      // Expand the resource list with resources on the current page index
      (bundle \ "entry").asInstanceOf[JArray].arr.foreach { entry =>
        currResources = currResources :+ (entry \ "resource").asInstanceOf[JObject]
      }
      if (hasNext.nonEmpty) { // If the response has the next page, increment the page index and then call getAllResources function again
        getAllResources(client, currResources, pageIndex + 1)
      } else { // If not, return current resource list
        Future {
          currResources
        }
      }
    }
  }

  def execute(client: FHIRClient): Future[JObject] = {
    client.searchByUrl(constructQueryString())
  }

}
