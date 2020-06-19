package uk.ac.wellcome.platform.storage.ingests.api

import java.net.URL

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.Materializer
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.messaging.sns.SNSConfig
import uk.ac.wellcome.messaging.typesafe.SNSBuilder
import uk.ac.wellcome.monitoring.typesafe.CloudWatchBuilder
import uk.ac.wellcome.platform.archive.common.config.builders.HTTPServerBuilder
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.platform.archive.common.http.{
  HttpMetrics,
  WellcomeHttpApp
}
import uk.ac.wellcome.platform.storage.ingests_tracker.client.{
  AkkaIngestTrackerClient,
  IngestTrackerClient
}
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()
    implicit val materializer: Materializer =
      AkkaBuilder.buildMaterializer()

    val httpServerConfigMain = HTTPServerBuilder.buildHTTPServerConfig(config)
    val contextURLMain = HTTPServerBuilder.buildContextURL(config)

    val ingestTrackerHost = Uri(
      config.requireString("ingests.tracker.host")
    )

    val router = new IngestsApi[SNSConfig] {
      override implicit val ec: ExecutionContext = executionContext
      override val ingestTrackerClient: IngestTrackerClient =
        new AkkaIngestTrackerClient(ingestTrackerHost)

      override val unpackerMessageSender: MessageSender[SNSConfig] =
        SNSBuilder.buildSNSMessageSender(
          config,
          namespace = "unpacker",
          subject = "Sent from the ingests API"
        )

      override val httpServerConfig: HTTPServerConfig = httpServerConfigMain
      override val contextURL: URL = contextURLMain
    }

    val appName = "IngestsApi"

    val httpMetrics = new HttpMetrics(
      name = appName,
      metrics = CloudWatchBuilder.buildCloudWatchMetrics(config)
    )

    new WellcomeHttpApp(
      routes = router.ingests,
      httpMetrics = httpMetrics,
      httpServerConfig = httpServerConfigMain,
      contextURL = contextURLMain,
      appName = appName
    )
  }
}
