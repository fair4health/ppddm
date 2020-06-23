package ppddm.core.rest.directive

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.{`Access-Control-Allow-Credentials`, `Access-Control-Allow-Headers`, `Access-Control-Allow-Methods`, `Access-Control-Allow-Origin`}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, RequestContext, Route, RouteResult}

import scala.concurrent.Future

trait CorsHandler {
  //this directive adds access control headers to normal responses
  private def addAccessControlHeaders: Directive0 = {
    mapResponseHeaders { headers =>
      `Access-Control-Allow-Origin`.* +:
        `Access-Control-Allow-Credentials`(true) +:
        `Access-Control-Allow-Headers`("Accept", "Accept-Encoding", "Accept-Language", "Authorization", "Content-Type", "Host", "Origin", "Referer", "User-Agent", "X-Requested-With") +:
        headers
    }
  }

  //this handles preflight OPTIONS requests.
  //otherwise has to be under addAccessControlHeaders
  private def preflightRequestHandler: Route = options {
    complete(HttpResponse(200).withHeaders(
      `Access-Control-Allow-Methods`(OPTIONS, POST, PUT, GET, DELETE)
    ))
  }

  def handleCORS(r: Route): RequestContext => Future[RouteResult] = addAccessControlHeaders {
    preflightRequestHandler ~ r
  }
}
