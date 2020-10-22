package ppddm.manager.client

import akka.Done
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{Accept, Authorization}
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal

import ppddm.core.rest.model._
import ppddm.manager.exception.AgentCommunicationException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

/* Import the ActorSystem */
import ppddm.manager.config.ManagerExecutionContext._

object AgentClient {

  // Default headers
  val defaultHeaders = List(Accept(MediaTypes.`application/json`))

  // TODO: How to obtain the access token? Discuss the authentication & authorization with ATOS.
  val accessToken: String = "token123"

  def createHttpRequest(agent: Agent, httpMethod: HttpMethod, uri: String, entity: Option[ModelClass] = None): AgentHttpRequest = {
    var request = HttpRequest(
      uri = Uri(uri),
      method = httpMethod,
      headers = AgentClient.defaultHeaders)
      .withHeaders(Authorization(headers.OAuth2BearerToken(AgentClient.accessToken)))

    /* To use the toJson, toPrettyJson methods of the JsonFormatter */
    import ppddm.core.util.JsonFormatter._

    if (entity.isDefined) {
      request = request.withEntity(ContentTypes.`application/json`, entity.get.toJson)
    }

    AgentHttpRequest(agent, request)
  }

  def invokeHttpRequest[T: TypeTag](agentHttpRequest: AgentHttpRequest): Future[Try[T]] = {
    /* So that we can Unmarshal to DataPreparationResult */
    import ppddm.core.rest.model.Json4sSupport._

    Http().singleRequest(agentHttpRequest.httpRequest).map(Try(_)) flatMap {
      case Success(res) =>
        res.status match {
          case StatusCodes.OK =>
            typeOf[T] match {
              case t if t =:= typeOf[Done] =>
                Future.apply(Success(Done.asInstanceOf[T]))
              case t if t =:= typeOf[DatasetSource] =>
                Future.apply(Success(DatasetSource(agentHttpRequest.agent, None, None, Some(ExecutionState.EXECUTING)).asInstanceOf[T]))
              case t if t =:= typeOf[DataPreparationResult] =>
                Unmarshal(res.entity).to[DataPreparationResult] map { r => Success(r.asInstanceOf[T]) }
              case t if t =:= typeOf[ModelTrainingResult] =>
                Unmarshal(res.entity).to[ModelTrainingResult] map { r => Success(r.asInstanceOf[T]) }
              case t if t =:= typeOf[ModelValidationResult] =>
                Unmarshal(res.entity).to[ModelValidationResult] map { r => Success(r.asInstanceOf[T]) }
              case t if t =:= typeOf[ModelTestResult] =>
                Unmarshal(res.entity).to[ModelTestResult] map { r => Success(r.asInstanceOf[T]) }
            }
          case _ =>
            // I got status code I didn't expect so I wrap it along with body into Future failure
            Unmarshal(res.entity).to[String].flatMap { body =>
              throw AgentCommunicationException(Some(agentHttpRequest.agent.name), Some(agentHttpRequest.agent.endpoint),
                s"The response status is ${res.status} [${agentHttpRequest.httpRequest.getUri()}] and response body is $body")
            }
        }
      case Failure(e) =>
        throw e
    } recover {
      case e: Exception =>
        Failure(AgentCommunicationException(Some(agentHttpRequest.agent.name), Some(agentHttpRequest.agent.endpoint), "Exception while connecting to the Agent", e))
    }
  }

}
