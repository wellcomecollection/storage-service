package uk.ac.wellcome.platform.archive.common.http

import java.net.URL
import java.util.UUID

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.directives.DebuggingDirectives
import akka.stream.ActorMaterializer
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class WellcomeHttpApp(
  routes: Route,
  httpServerConfig: HTTPServerConfig,
  val httpMetrics: HttpMetrics,
  val contextURL: URL,
  val appName: String
)(
  implicit
  val as: ActorSystem,
  val ec: ExecutionContext,
  mt: ActorMaterializer
) extends Runnable
    with WellcomeExceptionHandler
    with WellcomeRejectionHandler
    with Logging {

  private val appId = UUID.randomUUID()
  val appTag = s"$appName/$appId"

  import akka.http.scaladsl.server.Directives._

  def run(): Future[_] = {
    val handler = mapResponse { response =>
      httpMetrics.sendMetric(response)

      response
    }(routes)

    val logLevel = (appTag, Logging.InfoLevel)

    val loggedHandler = DebuggingDirectives
      .logRequestResult(logLevel)(handler)

    val binding = Http()
      .bindAndHandle(
        handler = loggedHandler,
        interface = httpServerConfig.host,
        port = httpServerConfig.port
      )

    info(
      s"$appTag - Starting: ${httpServerConfig.host}:${httpServerConfig.port}"
    )

    for {
      server <- binding
      _ = info(
        s"$appTag - Listening: ${httpServerConfig.host}:${httpServerConfig.port}"
      )
      _ <- server.whenTerminated
      _ = info(
        s"$appTag - Terminating: ${httpServerConfig.host}:${httpServerConfig.port}"
      )
    } yield server
  }
}
