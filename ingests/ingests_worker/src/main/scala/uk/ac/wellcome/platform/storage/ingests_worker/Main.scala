package uk.ac.wellcome.platform.storage.ingests_worker

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.typesafe.{
  AlpakkaSqsWorkerConfigBuilder,
  SQSBuilder
}
import uk.ac.wellcome.monitoring.cloudwatch.CloudWatchMetrics
import uk.ac.wellcome.monitoring.typesafe.CloudWatchBuilder
import uk.ac.wellcome.platform.storage.ingests_worker.services.IngestsWorkerService
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder
import com.typesafe.config.Config
import uk.ac.wellcome.platform.storage.ingests_tracker.client.AkkaIngestTrackerClient
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

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
