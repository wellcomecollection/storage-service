package uk.ac.wellcome.platform.storage.ingests_worker

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.Materializer
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.typesafe.{AlpakkaSqsWorkerConfigBuilder, CloudwatchMonitoringClientBuilder, SQSBuilder}
import uk.ac.wellcome.messaging.worker.monitoring.metrics.cloudwatch.CloudwatchMetricsMonitoringClient
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
    implicit val materializer: Materializer =
      AkkaBuilder.buildMaterializer()

    implicit val monitoringClient: CloudwatchMetricsMonitoringClient =
      CloudwatchMonitoringClientBuilder.buildCloudwatchMonitoringClient(config)

    implicit val sqsClient: SqsAsyncClient =
      SQSBuilder.buildSQSAsyncClient(config)

    val ingestTrackerHost = Uri(
      config.required[String]("ingests.tracker.host")
    )

    new IngestsWorkerService(
      workerConfig = AlpakkaSqsWorkerConfigBuilder.build(config),
      metricsNamespace = config.required[String]("aws.metrics.namespace"),
      ingestTrackerClient = new AkkaIngestTrackerClient(ingestTrackerHost)
    )
  }
}
