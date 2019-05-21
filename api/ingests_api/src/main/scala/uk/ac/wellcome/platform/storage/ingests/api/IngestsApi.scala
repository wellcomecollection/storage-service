package uk.ac.wellcome.platform.storage.ingests.api

import java.net.URL

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.platform.archive.common.http.{HttpMetrics, WellcomeHttpApp}
import uk.ac.wellcome.platform.archive.common.ingests.monitor.IngestTracker
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class IngestsApi[Destination](
  ingestTracker: IngestTracker,
  unpackerMessageSender: MessageSender[Destination],
  httpMetrics: HttpMetrics,
  httpServerConfig: HTTPServerConfig,
  contextURL: URL
)(implicit val actorSystem: ActorSystem,
  mat: ActorMaterializer,
  ec: ExecutionContext)
    extends Runnable {
  val router = new Router(
    ingestTracker = ingestTracker,
    ingestStarter = new IngestStarter[Destination](
      ingestTracker = ingestTracker,
      unpackerMessageSender = unpackerMessageSender
    ),
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
