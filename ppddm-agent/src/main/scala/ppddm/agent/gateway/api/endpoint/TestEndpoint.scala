package ppddm.agent.gateway.api.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ppddm.agent.controller.dm.TestController

trait TestEndpoint {

  def testRoute(implicit accessToken: String): Route = {
      pathPrefix("test") {
        pathPrefix("basicStatistics") {
          pathEndOrSingleSlash {
            get {
              complete {
                TestController.basicStatistics()
                StatusCodes.OK
              }
            }
          }
        } ~
          pathPrefix("correlations") {
            pathEndOrSingleSlash {
              get {
                complete {
                  TestController.correlations()
                  StatusCodes.OK
                }
              }
            }
          } ~
          pathPrefix("featureHasher") {
            pathEndOrSingleSlash {
              get {
                complete {
                  TestController.featureHasher()
                  StatusCodes.OK
                }
              }
            }
          } ~
          pathPrefix("pca") {
            pathEndOrSingleSlash {
              get {
                complete {
                  TestController.pca()
                  StatusCodes.OK
                }
              }
            }
          } ~
          pathPrefix("stringIndexer") {
            pathEndOrSingleSlash {
              get {
                complete {
                  TestController.stringIndexer()
                  StatusCodes.OK
                }
              }
            }
          } ~
          pathPrefix("oneHotEncoder") {
            pathEndOrSingleSlash {
              get {
                complete {
                  TestController.oneHotEncoder()
                  StatusCodes.OK
                }
              }
            }
          } ~
          pathPrefix("normalizer") {
            pathEndOrSingleSlash {
              get {
                complete {
                  TestController.normalizer()
                  StatusCodes.OK
                }
              }
            }
          } ~
          pathPrefix("vectorAssembler") {
            pathEndOrSingleSlash {
              get {
                complete {
                  TestController.vectorAssembler()
                  StatusCodes.OK
                }
              }
            }
          } ~
          pathPrefix("imputer") {
            pathEndOrSingleSlash {
              get {
                complete {
                  TestController.imputer()
                  StatusCodes.OK
                }
              }
            }
          } ~
          pathPrefix("logisticRegression") {
            pathEndOrSingleSlash {
              get {
                complete {
                  TestController.logisticRegression2()
                  StatusCodes.OK
                }
              }
            }
          }
      }
  }

}
