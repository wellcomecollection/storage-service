package uk.ac.wellcome.platform.storage.ingests.api

import java.net.URL

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import com.typesafe.config.Config
import uk.ac.wellcome.http.typesafe.HTTPServerBuilder
import weco.messaging.sns.SNSConfig
import weco.messaging.typesafe.SNSBuilder
import weco.monitoring.typesafe.CloudWatchBuilder
import uk.ac.wellcome.platform.storage.ingests.api.services.IngestCreator
import weco.storage_service.ingests_tracker.client.{
  AkkaIngestTrackerClient,
  IngestTrackerClient
}
import weco.typesafe.WellcomeTypesafeApp
import weco.typesafe.config.builders.AkkaBuilder
import weco.typesafe.config.builders.EnrichConfig._
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

    val client = new AkkaIngestTrackerClient(ingestTrackerHost)

    val router = new IngestsApi[SNSConfig] {
      override val ingestTrackerClient: IngestTrackerClient = client

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
