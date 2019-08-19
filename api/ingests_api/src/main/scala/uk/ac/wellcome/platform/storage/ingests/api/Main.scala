package uk.ac.wellcome.platform.storage.ingests.api

import java.net.URL

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.sns.SNSConfig
import uk.ac.wellcome.messaging.typesafe.SNSBuilder
import uk.ac.wellcome.monitoring.typesafe.MetricsBuilder
import uk.ac.wellcome.platform.archive.common.config.builders.HTTPServerBuilder
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.platform.archive.common.http.{HttpMetrics, WellcomeHttpApp}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.IngestTracker
import uk.ac.wellcome.platform.archive.common.ingests.tracker.dynamo.DynamoIngestTracker
import uk.ac.wellcome.platform.storage.ingests.api.services.IngestStarter
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
      metrics = MetricsBuilder.buildMetricsSender(config)
    )

    implicit val dynamoClient: AmazonDynamoDB =
      DynamoBuilder.buildDynamoClient(config)

    val ingestTrackerMain = new DynamoIngestTracker(
      config = DynamoBuilder.buildDynamoConfig(config),
      bagIdLookupConfig =
        DynamoBuilder.buildDynamoConfig(config, namespace = "bagIdLookup")
    )

    val ingestStarterMain = new IngestStarter[SNSConfig](
      ingestTracker = ingestTrackerMain,
      unpackerMessageSender = SNSBuilder.buildSNSMessageSender(
        config,
        namespace = "unpacker",
        subject = "Sent from the ingests API"
      )
    )

    val httpServerConfigMain = HTTPServerBuilder.buildHTTPServerConfig(config)
    val contextURLMain = HTTPServerBuilder.buildContextURL(config)

    val router = new IngestsApi {
      override val ingestTracker: IngestTracker = ingestTrackerMain
      override val ingestStarter: IngestStarter[_] = ingestStarterMain
      override val httpServerConfig: HTTPServerConfig = httpServerConfigMain
      override val contextURL: URL = contextURLMain
    }

    new WellcomeHttpApp(
      routes = router.ingests,
      httpMetrics = httpMetrics,
      httpServerConfig = httpServerConfigMain,
      contextURL = contextURLMain
    )
  }
}
