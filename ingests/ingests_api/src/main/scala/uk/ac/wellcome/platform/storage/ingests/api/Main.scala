package uk.ac.wellcome.platform.storage.ingests.api

import java.net.URL

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import com.typesafe.config.Config
import uk.ac.wellcome.http.typesafe.HTTPServerBuilder
import uk.ac.wellcome.messaging.sns.SNSConfig
import uk.ac.wellcome.messaging.typesafe.SNSBuilder
import uk.ac.wellcome.monitoring.typesafe.CloudWatchBuilder
import uk.ac.wellcome.platform.storage.ingests.api.services.IngestCreator
import uk.ac.wellcome.platform.storage.ingests_tracker.client.{
  AkkaIngestTrackerClient,
  IngestTrackerClient
}
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._
import weco.http.WellcomeHttpApp
import weco.http.models.HTTPServerConfig
import weco.http.monitoring.HttpMetrics

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()

    val httpServerConfigMain = HTTPServerBuilder.buildHTTPServerConfig(config)
    val contextURLMain = HTTPServerBuilder.buildContextURL(config)

    val ingestTrackerHost = Uri(
      config.requireString("ingests.tracker.host")
    )

    val ingestCreatorInstance = new IngestCreator(
      ingestTrackerClient = new AkkaIngestTrackerClient(ingestTrackerHost),
      unpackerMessageSender = SNSBuilder.buildSNSMessageSender(
        config,
        namespace = "unpacker",
        subject = "Sent from the ingests API"
      )
    )

    val router = new IngestsApi[SNSConfig] {
      override val ingestTrackerClient: IngestTrackerClient =
        new AkkaIngestTrackerClient(ingestTrackerHost)

      override val ingestCreator = ingestCreatorInstance

      override val httpServerConfig: HTTPServerConfig = httpServerConfigMain

      override def contextUrl: URL = contextURLMain

      override implicit val ec: ExecutionContext = executionContext
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
      appName = appName,
      contextUrl = contextURLMain
    )
  }
}
