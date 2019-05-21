package uk.ac.wellcome.platform.archive.bag_register

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.typesafe.config.Config
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.{AlpakkaSqsWorkerConfigBuilder, CloudwatchMonitoringClientBuilder, SQSBuilder}
import uk.ac.wellcome.messaging.worker.monitoring.CloudwatchMonitoringClient
import uk.ac.wellcome.platform.archive.bag_register.services.{BagRegisterWorker, Register}
import uk.ac.wellcome.platform.archive.common.config.builders.{IngestUpdaterBuilder, OperationNameBuilder, OutgoingPublisherBuilder}
import uk.ac.wellcome.platform.archive.common.dynamo._
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.{StorageManifestService, StorageManifestVHS}
import uk.ac.wellcome.storage.StorageBackend
import uk.ac.wellcome.storage.s3.S3StorageBackend
import uk.ac.wellcome.storage.typesafe.{S3Builder, VHSBuilder}
import uk.ac.wellcome.storage.vhs.EmptyMetadata
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()
    implicit val ec: ExecutionContext =
      AkkaBuilder.buildExecutionContext()
    implicit val materializer: ActorMaterializer =
      AkkaBuilder.buildActorMaterializer()

    implicit val s3Client: AmazonS3 = S3Builder.buildS3Client(config)

    implicit val monitoringClient: CloudwatchMonitoringClient =
      CloudwatchMonitoringClientBuilder.buildCloudwatchMonitoringClient(config)

    implicit val sqsClient: AmazonSQSAsync =
      SQSBuilder.buildSQSAsyncClient(config)

    implicit val s3StorageBackend: StorageBackend = new S3StorageBackend(s3Client)

    val storageManifestService = new StorageManifestService()

    val storageManifestVHS = new StorageManifestVHS(
      underlying = VHSBuilder.buildVHS[String, StorageManifest, EmptyMetadata](config)
    )

    val operationName = OperationNameBuilder
      .getName(config, default = "register")

    val ingestUpdater = IngestUpdaterBuilder.build(
      config,
      operationName
    )

    val register = new Register(
      storageManifestService,
      storageManifestVHS
    )

    val outgoingPublisher = OutgoingPublisherBuilder.build(
      config,
      operationName
    )

    new BagRegisterWorker(
      alpakkaSQSWorkerConfig = AlpakkaSqsWorkerConfigBuilder.build(config),
      ingestUpdater = ingestUpdater,
      outgoingPublisher = outgoingPublisher,
      register = register
    )
  }
}
