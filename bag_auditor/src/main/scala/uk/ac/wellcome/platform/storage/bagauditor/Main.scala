package uk.ac.wellcome.platform.storage.bagauditor

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.{AlpakkaSqsWorkerConfigBuilder, CloudwatchMonitoringClientBuilder, SQSBuilder}
import uk.ac.wellcome.messaging.worker.monitoring.CloudwatchMonitoringClient
import uk.ac.wellcome.platform.archive.common.config.builders.{IngestUpdaterBuilder, OperationNameBuilder, OutgoingPublisherBuilder}
import uk.ac.wellcome.platform.archive.common.versioning.{DynamoIngestVersionManagerDao, IngestVersionManager, IngestVersionManagerDao}
import uk.ac.wellcome.platform.storage.bagauditor.services.{BagAuditor, BagAuditorWorker}
import uk.ac.wellcome.platform.storage.bagauditor.versioning.VersionPicker
import uk.ac.wellcome.storage.typesafe.{DynamoBuilder, LockingBuilder, S3Builder}
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContextExecutor
import scala.util.Try

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem = AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContextExecutor =
      actorSystem.dispatcher
    implicit val mat: ActorMaterializer =
      AkkaBuilder.buildActorMaterializer()

    implicit val s3Client: AmazonS3 = S3Builder.buildS3Client(config)

    implicit val monitoringClient: CloudwatchMonitoringClient =
      CloudwatchMonitoringClientBuilder.buildCloudwatchMonitoringClient(config)

    implicit val sqsClient: AmazonSQSAsync =
      SQSBuilder.buildSQSAsyncClient(config)

    val operationName = OperationNameBuilder
      .getName(config, default = "auditing bag")

    val lockingService =
      LockingBuilder.buildDynamoLockingService[Int, Try](config)

    val ingestVersionManagerDao = new DynamoIngestVersionManagerDao(
      dynamoClient = DynamoBuilder.buildDynamoClient(config),
      dynamoConfig =
        DynamoBuilder.buildDynamoConfig(config, namespace = "versions")
    )

    val versionPicker = new VersionPicker(
      lockingService = lockingService,
      ingestVersionManager = new IngestVersionManager {
        override val dao: IngestVersionManagerDao = ingestVersionManagerDao
      }
    )

    new BagAuditorWorker(
      alpakkaSQSWorkerConfig = AlpakkaSqsWorkerConfigBuilder.build(config),
      bagAuditor = new BagAuditor(versionPicker = versionPicker),
      ingestUpdater = IngestUpdaterBuilder.build(config, operationName),
      outgoingPublisher = OutgoingPublisherBuilder.build(config, operationName)
    )
  }
}
