package ppddm.core.fhir.r4.service

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.HostConnectionPool
import akka.http.scaladsl.model.headers.{Accept, `Content-Type`}
import akka.http.scaladsl.model.{ContentTypes, HttpMethods, HttpRequest, HttpResponse, MediaTypes, StatusCodes, Uri}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.{OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
import com.typesafe.scalalogging.Logger
import ppddm.core.fhir.r4.resources.{Bundle, Condition, Resource}
import ppddm.core.rest.model.Json4sSupport._

import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS}
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global


/**
 * Client for communicating with a FHIR Repository
 *
 * @param host                FHIR Repository server host address i.e. http://localhost
 * @param port                FHIR Repository server port address i.e. 8282
 * @param path                FHIR Repository server path i.e. fhir
 * @param protocol            The communication protocol: http or https
 * @param poolSize            Size of the request pool in number of requests
 * @param requestWaitDuration Wait duration of each request in the pool
 * @param overflowStrategy    Overflow strategy to be used on the pool
 */
class FHIRClient(host: String,
                 port: Int,
                 path: String,
                 protocol: String,
                 poolSize: Int,
                 requestWaitDuration: Int,
                 overflowStrategy: OverflowStrategy)(implicit system: ActorSystem) {

  private val logger: Logger = Logger(this.getClass)

  // onFHIR.io server path
  private val serverPath = if(path.startsWith("/")) s"$host:$port$path" else s"$host:$port/$path"

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

  def query[T <: Resource](query: String)(implicit m: Manifest[T]): Future[Bundle[T]] = {
    logger.debug("Querying FHIR with {}", query)

    // Prepare http request
    val request = HttpRequest(
      uri = Uri(s"$protocol://$serverPath$query"),
      method = HttpMethods.GET,
      headers = defaultHeaders
    ).withEntity(ContentTypes.`application/json`, "{}")

    // This is actually queue request but cant call it to prevent infinite loop
    val responsePromise = Promise[HttpResponse]()
    val responseFuture = queue.offer(request -> responsePromise).flatMap {
      case QueueOfferResult.Enqueued => responsePromise.future
      case QueueOfferResult.Dropped => Future.failed(new RuntimeException("Queue overflowed. Try again later."))
      case QueueOfferResult.Failure(ex) => Future.failed(ex)
      case QueueOfferResult.QueueClosed => Future.failed(new RuntimeException("Queue was closed (pool shut down) while running the request. Try again later."))
    }

    // Execute token response and set the client access token parameter
    responseFuture.flatMap {
      case resp if resp.status == StatusCodes.OK =>
        Unmarshal(resp.entity).to[Bundle[T]]
      case err if err.entity.contentType == ContentTypes.`application/json` =>
        Unmarshal(err.entity).to[FHIRClientException] map { entity =>
          throw FHIRClientException(entity.error, entity.errorDesc)
        } recover { case _ => throw FHIRClientException() }
      case errUnk =>
        errUnk.entity.toStrict(FiniteDuration(1000, MILLISECONDS)).map {
          _.data
        }.map(_.utf8String) map { entity =>
          throw FHIRClientException(None, Some(s"Request failed. Response status is ${errUnk.status} and entity is $entity"))
        }
    }

  }
}

object FHIRClient {

  def apply(host: String,
            port: Int,
            path: String,
            protocol: String = "http",
            poolSize: Int = 64,
            requestWaitDuration: Int = 10,
            overflowStrategy: OverflowStrategy = OverflowStrategy.backpressure)(implicit system: ActorSystem): FHIRClient = {
    new FHIRClient(host, port, path, protocol, poolSize, requestWaitDuration, overflowStrategy)
  }

}
