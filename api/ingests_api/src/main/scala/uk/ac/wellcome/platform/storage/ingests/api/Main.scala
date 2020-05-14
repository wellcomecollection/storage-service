package uk.ac.wellcome.platform.storage.ingests.api

import java.net.URL

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.Materializer
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.sns.SNSConfig
import uk.ac.wellcome.messaging.typesafe.SNSBuilder
import uk.ac.wellcome.monitoring.typesafe.CloudWatchBuilder
import uk.ac.wellcome.platform.archive.common.config.builders.HTTPServerBuilder
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.platform.archive.common.http.{
  HttpMetrics,
  WellcomeHttpApp
}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.dynamo.DynamoIngestTracker
import uk.ac.wellcome.platform.storage.ingests.api.services.IngestStarter
import uk.ac.wellcome.platform.storage.ingests_tracker.client.{
  AkkaIngestTrackerClient,
  IngestTrackerClient
}
import uk.ac.wellcome.storage.typesafe.DynamoBuilder
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

    implicit val dynamoClient: AmazonDynamoDB =
      DynamoBuilder.buildDynamoClient(config)

    val ingestTrackerMain = new DynamoIngestTracker(
      config = DynamoBuilder.buildDynamoConfig(config)
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

    val ingestTrackerHost = Uri(
      config.required[String]("ingests.tracker.host")
    )

    val router = new IngestsApi {
      override implicit val ec: ExecutionContext = executionContext
      override val ingestTrackerClient: IngestTrackerClient = new AkkaIngestTrackerClient(ingestTrackerHost)

      override val ingestStarter: IngestStarter[_] = ingestStarterMain
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
