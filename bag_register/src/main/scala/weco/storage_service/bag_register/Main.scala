package weco.storage_service.bag_register

import akka.actor.ActorSystem
import com.amazonaws.services.s3.AmazonS3
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.json.JsonUtil._
import weco.messaging.typesafe.{
  AlpakkaSqsWorkerConfigBuilder,
  SNSBuilder,
  SQSBuilder
}
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage_service.bag_register.services.{
  BagRegisterWorker,
  Register,
  S3StorageManifestService
}
import weco.storage_service.bag_tracker.client.AkkaBagTrackerClient
import weco.storage_service.bagit.services.s3.S3BagReader
import weco.storage_service.config.builders.{
  IngestUpdaterBuilder,
  OperationNameBuilder
}
import weco.storage.typesafe.S3Builder
import weco.typesafe.WellcomeTypesafeApp
import weco.typesafe.config.builders.AkkaBuilder
import weco.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()
    implicit val ec: ExecutionContext =
      AkkaBuilder.buildExecutionContext()

    implicit val s3Client: AmazonS3 =
      S3Builder.buildS3Client

    implicit val metrics: CloudWatchMetrics =
      CloudWatchBuilder.buildCloudWatchMetrics(config)

    implicit val sqsClient: SqsAsyncClient =
      SQSBuilder.buildSQSAsyncClient

    val operationName = OperationNameBuilder.getName(config)

    val ingestUpdater = IngestUpdaterBuilder.build(config, operationName)

    val storageManifestService = new S3StorageManifestService()

    val register = new Register(
      bagReader = new S3BagReader(),
      bagTrackerClient = new AkkaBagTrackerClient(
        trackerHost = config.requireString("bags.tracker.host")
      ),
      storageManifestService = storageManifestService
    )

    val registrationNotifications = SNSBuilder.buildSNSMessageSender(
      config,
      namespace = "registration-notifications",
      subject = "Sent by the bag register"
    )

    new BagRegisterWorker(
      config = AlpakkaSqsWorkerConfigBuilder.build(config),
      ingestUpdater = ingestUpdater,
      registrationNotifications = registrationNotifications,
      register = register,
    )
  }
}
