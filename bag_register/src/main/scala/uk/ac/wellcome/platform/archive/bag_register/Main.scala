package uk.ac.wellcome.platform.archive.bag_register

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.amazonaws.services.s3.AmazonS3
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.{AlpakkaSqsWorkerConfigBuilder, CloudwatchMonitoringClientBuilder, SNSBuilder, SQSBuilder}
import uk.ac.wellcome.messaging.worker.monitoring.metrics.cloudwatch.CloudwatchMetricsMonitoringClient
import uk.ac.wellcome.platform.archive.bag_register.services.{BagRegisterWorker, Register, S3StorageManifestService}
import uk.ac.wellcome.platform.archive.bag_tracker.client.AkkaBagTrackerClient
import uk.ac.wellcome.platform.archive.common.bagit.services.s3.S3BagReader
import uk.ac.wellcome.platform.archive.common.config.builders.{IngestUpdaterBuilder, OperationNameBuilder}
import uk.ac.wellcome.storage.typesafe.S3Builder
import uk.ac.wellcome.storage.{ObjectLocationPrefix, S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()
    implicit val ec: ExecutionContext =
      AkkaBuilder.buildExecutionContext()
    implicit val mat: Materializer =
      AkkaBuilder.buildMaterializer()

    implicit val s3Client: AmazonS3 =
      S3Builder.buildS3Client(config)

    implicit val monitoringClient: CloudwatchMetricsMonitoringClient =
      CloudwatchMonitoringClientBuilder.buildCloudwatchMonitoringClient(config)

    implicit val sqsClient: SqsAsyncClient =
      SQSBuilder.buildSQSAsyncClient(config)

    val operationName = OperationNameBuilder.getName(config)

    val ingestUpdater = IngestUpdaterBuilder.build(
      config,
      operationName
    )

    val storageManifestService = new S3StorageManifestService()

    val register = new Register[S3ObjectLocation, S3ObjectLocationPrefix](
      bagReader = new S3BagReader(),
      bagTrackerClient = new AkkaBagTrackerClient(
        trackerHost = config.requireString("bags.tracker.host")
      ),
      storageManifestService = storageManifestService,
      toPrefix =
        (prefix: ObjectLocationPrefix) => S3ObjectLocationPrefix(prefix)
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
      metricsNamespace = config.requireString("aws.metrics.namespace")
    )
  }
}
