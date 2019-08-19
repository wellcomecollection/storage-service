package uk.ac.wellcome.platform.archive.common.http

import java.net.URL

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class WellcomeHttpApp(
  routes: Route,
  httpServerConfig: HTTPServerConfig,
  val httpMetrics: HttpMetrics,
  val contextURL: URL
)(
  implicit
  val as: ActorSystem,
  val ec: ExecutionContext,
  mt: ActorMaterializer
) extends Runnable
    with WellcomeExceptionHandler
    with WellcomeRejectionHandler
    with Logging {

  import akka.http.scaladsl.server.Directives._

  def run(): Future[_] = {
    val handler = mapResponse { response =>
      httpMetrics.sendMetric(response)

      response
    }(routes)

    val binding = Http()
      .bindAndHandle(
        handler = handler,
        interface = httpServerConfig.host,
        port = httpServerConfig.port
      )

    info(s"Starting: ${httpServerConfig.host}:${httpServerConfig.port}")

    for {
      server <- binding
      _ = info(s"Listening: ${httpServerConfig.host}:${httpServerConfig.port}")
      _ <- server.whenTerminated
      _ = info(
        s"Terminating: ${httpServerConfig.host}:${httpServerConfig.port}"
      )
    } yield server
  }
}
