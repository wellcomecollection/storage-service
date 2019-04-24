package uk.ac.wellcome.platform.archive.bagunpacker

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.CloudwatchMonitoringClientBuilder._
import uk.ac.wellcome.messaging.typesafe.SQSBuilder.buildSQSAsyncClient
import uk.ac.wellcome.messaging.worker.monitoring.CloudwatchMonitoringClient
import uk.ac.wellcome.platform.archive.bagunpacker.config.builders.{
  AlpakkaSqsWorkerConfigBuilder,
  UnpackerWorkerConfigBuilder
}
import uk.ac.wellcome.platform.archive.bagunpacker.services.{
  BagUnpackerWorker,
  S3Uploader,
  Unpacker
}
import uk.ac.wellcome.platform.archive.common.config.builders.{
  IngestUpdaterBuilder,
  OperationNameBuilder,
  OutgoingPublisherBuilder
}
import uk.ac.wellcome.storage.typesafe.S3Builder._
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder._

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      buildActorSystem()

    implicit val materializer: ActorMaterializer =
      buildActorMaterializer()

    implicit val executionContext: ExecutionContext =
      buildExecutionContext()

    implicit val s3Client: AmazonS3 =
      buildS3Client(config)

    implicit val monitoringClient: CloudwatchMonitoringClient =
      buildCloudwatchMonitoringClient(config)

    implicit val sqsClient: AmazonSQSAsync =
      buildSQSAsyncClient(config)

    val alpakkaSQSWorkerConfig =
      AlpakkaSqsWorkerConfigBuilder.build(config)

    val unpackerWorkerConfig =
      UnpackerWorkerConfigBuilder.build(config)

    val operationName = OperationNameBuilder
      .getName(config, default = "unpacking")

    val ingestUpdater =
      IngestUpdaterBuilder.build(config, operationName = operationName)

    val outgoingPublisher =
      OutgoingPublisherBuilder.build(config, operationName = operationName)

    BagUnpackerWorker(
      alpakkaSQSWorkerConfig = alpakkaSQSWorkerConfig,
      bagUnpackerWorkerConfig = unpackerWorkerConfig,
      ingestUpdater = ingestUpdater,
      outgoingPublisher = outgoingPublisher,
      unpacker = Unpacker(s3Uploader = new S3Uploader())
    )
  }
}
