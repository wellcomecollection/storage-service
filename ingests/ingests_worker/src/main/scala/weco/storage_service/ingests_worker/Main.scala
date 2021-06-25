package weco.storage_service.ingests_worker

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.messaging.typesafe.{AlpakkaSqsWorkerConfigBuilder, SQSBuilder}
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage_service.ingests_worker.services.IngestsWorkerService
import weco.typesafe.WellcomeTypesafeApp
import weco.typesafe.config.builders.AkkaBuilder
import com.typesafe.config.Config
import weco.storage_service.ingests_tracker.client.AkkaIngestTrackerClient
import weco.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()

    implicit val metrics: CloudWatchMetrics =
      CloudWatchBuilder.buildCloudWatchMetrics(config)

    implicit val sqsClient: SqsAsyncClient =
      SQSBuilder.buildSQSAsyncClient(config)

    val ingestTrackerHost = Uri(
      config.requireString("ingests.tracker.host")
    )

    new IngestsWorkerService(
      workerConfig = AlpakkaSqsWorkerConfigBuilder.build(config),
      metricsNamespace = config.requireString("aws.metrics.namespace"),
      ingestTrackerClient = new AkkaIngestTrackerClient(ingestTrackerHost)
    )
  }
}
