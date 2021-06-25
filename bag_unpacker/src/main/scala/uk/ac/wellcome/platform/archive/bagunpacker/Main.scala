package uk.ac.wellcome.platform.archive.bagunpacker

import akka.actor.ActorSystem
import com.amazonaws.services.s3.AmazonS3
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.json.JsonUtil._
import weco.messaging.typesafe.AlpakkaSqsWorkerConfigBuilder
import weco.messaging.typesafe.SQSBuilder.buildSQSAsyncClient
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import uk.ac.wellcome.platform.archive.bagunpacker.config.builders.UnpackerWorkerConfigBuilder
import uk.ac.wellcome.platform.archive.bagunpacker.services.BagUnpackerWorker
import uk.ac.wellcome.platform.archive.bagunpacker.services.s3.S3Unpacker
import weco.storage_service.config.builders.{
  IngestUpdaterBuilder,
  OperationNameBuilder,
  OutgoingPublisherBuilder
}
import weco.storage.typesafe.S3Builder._
import weco.typesafe.WellcomeTypesafeApp
import weco.typesafe.config.builders.AkkaBuilder
import weco.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()

    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()

    implicit val s3Client: AmazonS3 =
      buildS3Client(config)

    implicit val metrics: CloudWatchMetrics =
      CloudWatchBuilder.buildCloudWatchMetrics(config)

    implicit val sqsClient: SqsAsyncClient =
      buildSQSAsyncClient(config)

    val alpakkaSQSWorkerConfig =
      AlpakkaSqsWorkerConfigBuilder.build(config)

    val unpackerWorkerConfig =
      UnpackerWorkerConfigBuilder.build(config)

    val operationName = OperationNameBuilder.getName(config)

    val ingestUpdater =
      IngestUpdaterBuilder.build(config, operationName = operationName)

    val outgoingPublisher =
      OutgoingPublisherBuilder.build(config, operationName = operationName)

    new BagUnpackerWorker(
      config = alpakkaSQSWorkerConfig,
      bagUnpackerWorkerConfig = unpackerWorkerConfig,
      ingestUpdater = ingestUpdater,
      outgoingPublisher = outgoingPublisher,
      unpacker = new S3Unpacker(),
      metricsNamespace = config.requireString("aws.metrics.namespace")
    )
  }
}
