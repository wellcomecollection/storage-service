package uk.ac.wellcome.platform.archive.bag_register

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.{
  AlpakkaSqsWorkerConfigBuilder,
  CloudwatchMonitoringClientBuilder,
  SQSBuilder
}
import uk.ac.wellcome.messaging.worker.monitoring.CloudwatchMonitoringClient
import uk.ac.wellcome.platform.archive.bag_register.services.{
  BagRegisterWorker,
  Register
}
import uk.ac.wellcome.platform.archive.common.bagit.services.s3.S3BagReader
import uk.ac.wellcome.platform.archive.common.config.builders.{
  IngestUpdaterBuilder,
  OperationNameBuilder,
  OutgoingPublisherBuilder,
  StorageManifestDaoBuilder
}
import uk.ac.wellcome.storage.typesafe.S3Builder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContext
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.storage.services.{
  S3SizeFinder,
  StorageManifestService
}

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()
    implicit val ec: ExecutionContext =
      AkkaBuilder.buildExecutionContext()
    implicit val mat: ActorMaterializer =
      AkkaBuilder.buildActorMaterializer()

    implicit val s3Client: AmazonS3 = S3Builder.buildS3Client(config)

    implicit val monitoringClient: CloudwatchMonitoringClient =
      CloudwatchMonitoringClientBuilder.buildCloudwatchMonitoringClient(config)

    implicit val sqsClient: AmazonSQSAsync =
      SQSBuilder.buildSQSAsyncClient(config)

    val storageManifestVHS = StorageManifestDaoBuilder.build(config)

    val operationName = OperationNameBuilder.getName(config)

    val ingestUpdater = IngestUpdaterBuilder.build(
      config,
      operationName
    )

    val storageManifestService = new StorageManifestService(
      sizeFinder = new S3SizeFinder()
    )

    val register = new Register(
      bagReader = new S3BagReader(),
      storageManifestDao = storageManifestVHS,
      storageManifestService = storageManifestService
    )

    val outgoingPublisher = OutgoingPublisherBuilder.build(
      config,
      operationName
    )

    new BagRegisterWorker(
      config = AlpakkaSqsWorkerConfigBuilder.build(config),
      ingestUpdater = ingestUpdater,
      outgoingPublisher = outgoingPublisher,
      register = register
    )
  }
}
