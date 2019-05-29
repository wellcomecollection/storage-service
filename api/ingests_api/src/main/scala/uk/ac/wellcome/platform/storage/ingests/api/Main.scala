package uk.ac.wellcome.platform.storage.ingests.api

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.sns.SNSConfig
import uk.ac.wellcome.messaging.typesafe.SNSBuilder
import uk.ac.wellcome.monitoring.typesafe.MetricsBuilder
import uk.ac.wellcome.platform.archive.common.config.builders.HTTPServerBuilder
import uk.ac.wellcome.platform.archive.common.http.HttpMetrics
import uk.ac.wellcome.platform.archive.common.ingests.monitor.IngestTracker
import uk.ac.wellcome.storage.typesafe.DynamoBuilder
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

    val httpMetrics = new HttpMetrics(
      name = "IngestsApi",
      metricsSender = MetricsBuilder.buildMetricsSender(config)
    )

    val ingestTracker = new IngestTracker(
      dynamoClient = DynamoBuilder.buildDynamoClient(config),
      dynamoConfig = DynamoBuilder.buildDynamoConfig(config)
    )

    val ingestStarter = new IngestStarter[SNSConfig](
      ingestTracker = ingestTracker,
      unpackerMessageSender = SNSBuilder.buildSNSMessageSender(
        config,
        namespace = "unpacker",
        subject = "Sent from the ingests API"
      )
    )

    new IngestsApi(
      ingestTracker = ingestTracker,
      ingestStarter = ingestStarter,
      httpMetrics = httpMetrics,
      httpServerConfig = HTTPServerBuilder.buildHTTPServerConfig(config),
      contextURL = HTTPServerBuilder.buildContextURL(config)
    )
  }
}
