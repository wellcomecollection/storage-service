package uk.ac.wellcome.platform.storage.bag_root_finder

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.amazonaws.services.s3.AmazonS3
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.{AlpakkaSqsWorkerConfigBuilder, CloudwatchMonitoringClientBuilder, SQSBuilder}
import uk.ac.wellcome.messaging.worker.monitoring.metrics.cloudwatch.CloudwatchMetricsMonitoringClient
import uk.ac.wellcome.platform.archive.common.config.builders.{IngestUpdaterBuilder, OperationNameBuilder, OutgoingPublisherBuilder}
import uk.ac.wellcome.platform.storage.bag_root_finder.services.{BagRootFinder, BagRootFinderWorker}
import uk.ac.wellcome.storage.typesafe.S3Builder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContextExecutor

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem = AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContextExecutor =
      actorSystem.dispatcher
    implicit val mat: Materializer =
      AkkaBuilder.buildMaterializer()

    implicit val s3Client: AmazonS3 = S3Builder.buildS3Client(config)

    implicit val monitoringClient: CloudwatchMetricsMonitoringClient =
      CloudwatchMonitoringClientBuilder.buildCloudwatchMonitoringClient(config)

    implicit val sqsClient: SqsAsyncClient =
      SQSBuilder.buildSQSAsyncClient(config)

    val operationName = OperationNameBuilder.getName(config)

    new BagRootFinderWorker(
      config = AlpakkaSqsWorkerConfigBuilder.build(config),
      bagRootFinder = new BagRootFinder(),
      ingestUpdater = IngestUpdaterBuilder.build(config, operationName),
      outgoingPublisher = OutgoingPublisherBuilder.build(config, operationName),
      metricsNamespace = config.required[String]("aws.metrics.namespace")
    )
  }
}
