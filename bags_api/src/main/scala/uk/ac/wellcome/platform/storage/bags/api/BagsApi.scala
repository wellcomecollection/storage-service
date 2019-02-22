package uk.ac.wellcome.platform.storage.bags.api

import java.net.URL

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.platform.archive.common.http.{HttpMetrics, WellcomeHttpApp}
import uk.ac.wellcome.platform.archive.common.storage.StorageManifestVHS
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class BagsApi(
  vhs: StorageManifestVHS,
  httpMetrics: HttpMetrics,
  httpServerConfig: HTTPServerConfig,
  contextURL: URL
)(implicit val actorSystem: ActorSystem,
  materializer: ActorMaterializer,
  ec: ExecutionContext)
    extends Logging
    with Runnable {

  val router = new Router(
    vhs = vhs,
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
