package ppddm.core.fhir

case class FHIRQueryWithQueryString(query: String, fhirPath: Option[String] = None) extends FHIRQuery(fhirPath) {

  private val newQuery:String = if(query.contains("?")) query + "&" else query + "?"

  override def constructQueryString(): String = {
    query
  }

  override def getCountQuery(): FHIRQuery = {
    this.copy(newQuery + "_summary=count")
  }

  override def getPageQuery(count: Int, pageIndex: Int): FHIRQuery = {
    this.copy(newQuery + s"_count=${count}&_page=${pageIndex}")
  }

}
