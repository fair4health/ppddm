package ppddm.core.fhir

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.HostConnectionPool
import akka.http.scaladsl.model.headers.Accept
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
import akka.stream.{OverflowStrategy, QueueOfferResult}
import com.typesafe.scalalogging.Logger
import org.json4s.JObject
import ppddm.core.exception.FHIRClientException
import ppddm.core.rest.model.Json4sSupport._
import ppddm.core.util.URLUtil

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class FHIRClient(host: String,
                 port: Int,
                 path: String,
                 protocol: String,
                 poolSize: Int,
                 overflowStrategy: OverflowStrategy)(implicit system: ActorSystem) {

  private val logger: Logger = Logger(this.getClass)

  // onFHIR.io server path
  private val fhirServerBaseURI = URLUtil.append(s"$protocol://$host:$port", path)

  // Default headers
  private val defaultHeaders = List(Accept(MediaTypes.`application/json`))

  // Connection pool and queue for handling Http Requests
  private val poolClientFlow: Flow[(HttpRequest, Promise[HttpResponse]), (Try[HttpResponse], Promise[HttpResponse]), HostConnectionPool] =
    if (protocol == "https") Http().cachedHostConnectionPoolHttps[Promise[HttpResponse]](host, port)
    else Http().cachedHostConnectionPool[Promise[HttpResponse]](host, port)

  // Http request to response queue
  private val queue: SourceQueueWithComplete[(HttpRequest, Promise[HttpResponse])] =
    Source.queue[(HttpRequest, Promise[HttpResponse])](poolSize, overflowStrategy)
      .via(poolClientFlow)
      .toMat(Sink.foreach({
        case (Success(resp), p) => p.success(resp)
        case (Failure(e), p) => p.failure(e)
      }))(Keep.left)
      .run()

  def searchByUrl(query: String): Future[JObject] = {
    logger.debug("Querying FHIR with {}", query)
    //Separate the resource type and parameters
    val resourceTypeAndParams = query.split("\\?")
    val resourceType = Try(resourceTypeAndParams(0)).getOrElse("")
    val params = Try(resourceTypeAndParams(1)).getOrElse("")

    // Prepare http request
    val request = HttpRequest(
      uri = Uri(s"$fhirServerBaseURI$resourceType" + "/_search"), // use /_search endpoint to expand query size
      method = HttpMethods.POST,
      headers = defaultHeaders
    ).withEntity(ContentTypes.`application/x-www-form-urlencoded`, params)

    // This is actually queue request but cant call it to prevent infinite loop
    val responsePromise = Promise[HttpResponse]()
    val responseFuture = queue.offer(request -> responsePromise).flatMap {
      case QueueOfferResult.Enqueued => responsePromise.future
      case QueueOfferResult.Dropped => Future.failed(new RuntimeException("Queue overflowed. Try again later."))
      case QueueOfferResult.Failure(ex) => Future.failed(ex)
      case QueueOfferResult.QueueClosed => Future.failed(new RuntimeException("Queue was closed (pool shut down) while running the request. Try again later."))
    }

    // Process the FHIR response and return as a JObject
    responseFuture.flatMap {
      case resp if resp.status == StatusCodes.OK =>
        Unmarshal(resp.entity).to[JObject]
      case errUnk =>
        errUnk.entity.toStrict(FiniteDuration(1000, MILLISECONDS)).map(_.data.utf8String).map { entity =>
          throw FHIRClientException(entity)
        }
    }
  }
}

object FHIRClient {
  def apply(host: String,
            port: Int,
            path: String,
            protocol: String = "http")(implicit system: ActorSystem): FHIRClient = {
    new FHIRClient(host, port, path, protocol, 64, OverflowStrategy.backpressure)
  }
}
