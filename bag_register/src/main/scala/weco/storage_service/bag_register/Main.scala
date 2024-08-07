package weco.storage_service.bag_register

import org.apache.pekko.actor.ActorSystem
import com.typesafe.config.Config
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.json.JsonUtil._
import weco.messaging.typesafe.{PekkoSQSWorkerConfigBuilder, SNSBuilder}
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage_service.bag_register.services.{
  BagRegisterWorker,
  Register,
  S3StorageManifestService
}
import weco.storage_service.bag_tracker.client.PekkoBagTrackerClient
import weco.storage_service.bagit.services.s3.S3BagReader
import weco.storage_service.config.builders.{
  IngestUpdaterBuilder,
  OperationNameBuilder
}
import weco.typesafe.WellcomeTypesafeApp
import weco.typesafe.config.builders.EnrichConfig._

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

    val operationName = OperationNameBuilder.getName(config)

    val ingestUpdater = IngestUpdaterBuilder.build(config, operationName)

    val storageManifestService = new S3StorageManifestService()

    val register = new Register(
      bagReader = new S3BagReader(),
      bagTrackerClient = new PekkoBagTrackerClient(
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
      config = PekkoSQSWorkerConfigBuilder.build(config),
      ingestUpdater = ingestUpdater,
      registrationNotifications = registrationNotifications,
      register = register
    )
  }
}
