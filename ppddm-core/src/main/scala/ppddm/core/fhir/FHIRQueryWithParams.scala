package ppddm.core.fhir

/**
 * This class represents a bare FHIR query. The query is represented for the resourceType using the params
 *
 * @param resourceType
 * @param params
 */
case class FHIRQueryWithParams(resourceType: String, params: Seq[(String, String)], fhirPath: Option[String] = None) extends FHIRQuery(fhirPath) {

  override def constructQueryString(): String = {
    var query = s"/${resourceType}"

    if (params.nonEmpty)
      query = query + "?" + params.map(p => s"${p._1}=${p._2}").mkString("&")

    query
  }

  override def getCountQuery(): FHIRQuery = {
    this.copy(resourceType, params :+ ("_summary" -> "count"))
  }

  override def getPageQuery(count: Int, pageIndex: Int): FHIRQuery = {
    this.copy(resourceType, params ++ Seq("_count" -> count.toString, "_page" -> pageIndex.toString))
  }

}
