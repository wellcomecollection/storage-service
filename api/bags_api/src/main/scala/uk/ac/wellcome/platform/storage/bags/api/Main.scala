package uk.ac.wellcome.platform.storage.bags.api

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.Config
import uk.ac.wellcome.monitoring.typesafe.MetricsBuilder
import uk.ac.wellcome.platform.archive.common.config.builders._
import uk.ac.wellcome.platform.archive.common.http.HttpMetrics
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()
    implicit val materializer: ActorMaterializer =
      AkkaBuilder.buildActorMaterializer()

    val vhs = StorageManifestDaoBuilder.build(config)

    val httpMetrics = new HttpMetrics(
      name = "BagsApi",
      metricsSender = MetricsBuilder.buildMetricsSender(config)
    )

    new BagsApi(
      vhs = vhs,
      httpMetrics = httpMetrics,
      httpServerConfig = HTTPServerBuilder.buildHTTPServerConfig(config),
      contextURL = HTTPServerBuilder.buildContextURL(config)
    )
  }
}
