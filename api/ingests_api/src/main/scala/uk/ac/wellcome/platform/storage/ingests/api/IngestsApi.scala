package uk.ac.wellcome.platform.storage.ingests.api

import java.net.URL

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.platform.archive.common.http.{
  HttpMetrics,
  WellcomeHttpApp
}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.IngestTracker
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class IngestsApi[UnpackerDestination](
  ingestTracker: IngestTracker,
  ingestStarter: IngestStarter[UnpackerDestination],
  httpMetrics: HttpMetrics,
  httpServerConfig: HTTPServerConfig,
  contextURL: URL
)(
  implicit val actorSystem: ActorSystem,
  mat: ActorMaterializer,
  ec: ExecutionContext
) extends Runnable {

  val router = new Router(
    ingestTracker = ingestTracker,
    ingestStarter = ingestStarter,
    httpServerConfig = httpServerConfig,
    contextURL = contextURL
  )

  val app = new WellcomeHttpApp(
    routes = router.routes,
    httpMetrics = httpMetrics,
    httpServerConfig = httpServerConfig,
    contextURL = contextURL
  )

  def run(): Future[Http.HttpTerminated] =
    app.run()
}
