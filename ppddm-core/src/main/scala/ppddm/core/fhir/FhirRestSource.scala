package ppddm.core.fhir

import ca.uhn.fhir.context.{FhirContext, FhirVersionEnum}
import ca.uhn.fhir.parser.IParser
import ca.uhn.fhir.rest.client.api.{IGenericClient, IRestfulClientFactory, ServerValidationModeEnum}
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.hl7.fhir.instance.model.api.IBaseResource
import org.hl7.fhir.r4.model.DomainResource

import scala.reflect.ClassTag
import scala.util.Try


class FhirRestSource(host: String,
                     port: Int,
                     path: String,
                     protocol: String = "http",
                     fhirVersionEnum: FhirVersionEnum = FhirVersionEnum.R4,
                     batchSize:Int = 100) extends Serializable {

  @transient lazy val logger = LogManager.getLogger("transformerLogger")

  @transient
  var fhirContext:FhirContext = getFhirContext()

  /**
   * Client factory to create HAPI Fhir client
   */
  @transient
  lazy val clientFactory: IRestfulClientFactory = getClientFactory()

  // onFHIR.io server path
  private val fhirServerBaseURI = if(path.startsWith("/")) s"$protocol://$host:$port$path" else s"$protocol://$host:$port/$path"

  /**
   * Gerenate a fhir context
   * @return
   */
  def getFhirContext():FhirContext = {
    fhirVersionEnum match {
      case FhirVersionEnum.DSTU2 => FhirContext.forDstu2()
      case FhirVersionEnum.DSTU3 => FhirContext.forDstu3()
      case FhirVersionEnum.R4 => FhirContext.forR4()
      //TODO correct the exception
      case _ => throw new Exception("FHIR version not supported!!!")
    }
  }

  /**
   * Generate the HAPI Client factory
   * @return
   */
  private def getClientFactory():IRestfulClientFactory = {
    val fhirContext = getFhirContext()

    val factory = fhirContext.getRestfulClientFactory
    factory.setServerValidationMode(ServerValidationModeEnum.NEVER)
    factory.setConnectTimeout(300000)
    factory.setSocketTimeout(300000)
    factory
  }

  /**
   * Generate the HAPI Client
   * @return
   */
  private def getClient():IGenericClient = {
    clientFactory.newGenericClient(fhirServerBaseURI)
  }

  /**
   *
   * @param sparkSession
   * @param query
   * @param evidence
   * @tparam T
   * @return
   */
  def runDistributedFhirQuery[T<: DomainResource](sparkSession: SparkSession, query:FhirQuery)(implicit evidence:ClassTag[T]):RDD[T] = {
    val fhirClient = getClient()
    //First count the results for the query
    //IMPORTANT do not catch exception so we can exit if we have problem
    /*val numOfResults:Int = Try(query.getCount(fhirClient)).recover {
      case t:Throwable =>
        logger.error("Problem in fhir query(count)", t)
        0
    }.get*/

    val numOfResults:Int = query.getCount(fhirClient)
    if(numOfResults > 0) {
      //Number of pages to get all the results according to batch size
      val numOfReturnPagesForQuery = numOfResults / batchSize + 1
      //Parallize the execution and get pages in parallel
      sparkSession.sparkContext.parallelize(1 to numOfReturnPagesForQuery).mapPartitions[T](partitionPages => {
        val fhirClient = getClient()
        val resources = partitionPages.flatMap(page => {
          Try(query.getResources[T](fhirClient, batchSize, page)).recover {
            case t:Throwable =>
              logger.error("Problem in FHIR query", t)
              Nil
          }.get
        })
        resources
      })
    } else sparkSession.sparkContext.parallelize(Nil)
  }

  /**
   * Transform sequence of JSON resources with same type to HAPI class
   * @param resource
   * @param ct
   * @tparam T
   * @return
   */
  private def transform[T<:IBaseResource](jsonParser:IParser,key:String, resource:String)(implicit ct:ClassTag[T]):Option[(String, T)] = {
    Try {
      key -> jsonParser.parseResource(resource).asInstanceOf[T]
    }.toOption
  }
}

