package weco.storage_service.ingests_worker

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.messaging.typesafe.AlpakkaSqsWorkerConfigBuilder
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage_service.ingests_tracker.client.AkkaIngestTrackerClient
import weco.storage_service.ingests_worker.services.IngestsWorkerService
import weco.typesafe.WellcomeTypesafeApp
import weco.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      ActorSystem("main-actor-system")
    implicit val ec: ExecutionContext =
      actorSystem.dispatcher

    implicit val metrics: CloudWatchMetrics =
      CloudWatchBuilder.buildCloudWatchMetrics(config)

    implicit val sqsClient: SqsAsyncClient =
      SqsAsyncClient.builder().build()

    val ingestTrackerHost = Uri(
      config.requireString("ingests.tracker.host")
    )

    new IngestsWorkerService(
      config = AlpakkaSqsWorkerConfigBuilder.build(config),
      ingestTrackerClient = new AkkaIngestTrackerClient(ingestTrackerHost)
    )
  }
}
