package weco.storage_service.bag_unpacker

import akka.actor.ActorSystem
import com.typesafe.config.Config
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.transfer.s3.S3TransferManager
import weco.json.JsonUtil._
import weco.messaging.typesafe.AlpakkaSqsWorkerConfigBuilder
import weco.messaging.typesafe.SQSBuilder.buildSQSAsyncClient
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage_service.bag_unpacker.config.builders.UnpackerWorkerConfigBuilder
import weco.storage_service.bag_unpacker.services.BagUnpackerWorker
import weco.storage_service.bag_unpacker.services.s3.S3Unpacker
import weco.storage_service.config.builders.{
  IngestUpdaterBuilder,
  OperationNameBuilder,
  OutgoingPublisherBuilder
}
import weco.typesafe.WellcomeTypesafeApp
import weco.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()

    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()

    implicit val s3Client: S3Client =
      S3Client.builder().build()

    implicit val s3TransferManager: S3TransferManager =
      S3TransferManager.builder().build()

    implicit val metrics: CloudWatchMetrics =
      CloudWatchBuilder.buildCloudWatchMetrics(config)

    implicit val sqsClient: SqsAsyncClient =
      buildSQSAsyncClient

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
      unpacker = new S3Unpacker()
    )
  }
}
