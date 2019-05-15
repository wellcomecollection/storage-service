package uk.ac.wellcome.platform.archive.bagverifier

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.{AlpakkaSqsWorkerConfigBuilder, CloudwatchMonitoringClientBuilder, SQSBuilder}
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bagverifier.services.{BagVerifier, BagVerifierWorker, S3ObjectVerifier}
import uk.ac.wellcome.platform.archive.common.bagit.services.BagService
import uk.ac.wellcome.platform.archive.common.config.builders.{IngestUpdaterBuilder, OperationNameBuilder, OutgoingPublisherBuilder}
import uk.ac.wellcome.storage.typesafe.S3Builder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()
    implicit val materializer: ActorMaterializer =
      AkkaBuilder.buildActorMaterializer()

    implicit val s3Client: AmazonS3 =
      S3Builder.buildS3Client(config)
    implicit val monitoringClient: MonitoringClient =
      CloudwatchMonitoringClientBuilder.buildCloudwatchMonitoringClient(config)
    implicit val sqsClient: AmazonSQSAsync =
      SQSBuilder.buildSQSAsyncClient(config)
    implicit val s3ObjectVerifier =
      new S3ObjectVerifier()
    implicit val bagService =
      new BagService()

    val verifier = new BagVerifier()

    val operationName = OperationNameBuilder
      .getName(config, default = "verification")

    val ingestUpdater = IngestUpdaterBuilder.build(config, operationName)

    val outgoingPublisher =
      OutgoingPublisherBuilder.build(config, operationName)

    new BagVerifierWorker(
      alpakkaSQSWorkerConfig = AlpakkaSqsWorkerConfigBuilder.build(config),
      ingestUpdater = ingestUpdater,
      outgoingPublisher = outgoingPublisher,
      verifier = verifier
    )
  }
}
