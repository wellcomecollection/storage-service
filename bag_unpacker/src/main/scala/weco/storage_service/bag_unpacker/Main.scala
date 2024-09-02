package weco.storage_service.bag_unpacker

import org.apache.pekko.actor.ActorSystem
import com.typesafe.config.Config
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.json.JsonUtil._
import weco.messaging.typesafe.PekkoSQSWorkerConfigBuilder
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

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      ActorSystem("main-actor-system")

    implicit val ec: ExecutionContext =
      actorSystem.dispatcher

    implicit val s3Client: S3Client =
      S3Client.builder().build()

    implicit val metrics: CloudWatchMetrics =
      CloudWatchBuilder.buildCloudWatchMetrics(config)

    implicit val sqsClient: SqsAsyncClient =
      SqsAsyncClient.builder().build()

    val PekkoSQSWorkerConfig =
      PekkoSQSWorkerConfigBuilder.build(config)

    val unpackerWorkerConfig =
      UnpackerWorkerConfigBuilder.build(config)

    val operationName = OperationNameBuilder.getName(config)

    val ingestUpdater =
      IngestUpdaterBuilder.build(config, operationName = operationName)

    val outgoingPublisher =
      OutgoingPublisherBuilder.build(config, operationName = operationName)

    new BagUnpackerWorker(
      config = PekkoSQSWorkerConfig,
          bagUnpackerWorkerConfig = unpackerWorkerConfig,
      ingestUpdater = ingestUpdater,
      outgoingPublisher = outgoingPublisher,
      unpacker = new S3Unpacker()
    )
  }
}
