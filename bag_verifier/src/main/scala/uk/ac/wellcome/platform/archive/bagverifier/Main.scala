package uk.ac.wellcome.platform.archive.bagverifier

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.{AlpakkaSqsWorkerConfigBuilder, CloudwatchMonitoringClientBuilder, SQSBuilder}
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bagverifier.services.{BagVerifier, BagVerifierWorker}
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.bagit.services.s3.S3BagReader
import uk.ac.wellcome.platform.archive.common.config.builders.{IngestUpdaterBuilder, OperationNameBuilder, OutgoingPublisherBuilder}
import uk.ac.wellcome.platform.archive.common.storage.services.S3Resolvable
import uk.ac.wellcome.storage.typesafe.S3Builder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContext
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.verify.s3.S3ObjectVerifier

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()

    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()

    implicit val mat: ActorMaterializer =
      AkkaBuilder.buildActorMaterializer()

    implicit val s3Client: AmazonS3 =
      S3Builder.buildS3Client(config)

    implicit val monitoringClient: MonitoringClient =
      CloudwatchMonitoringClientBuilder.buildCloudwatchMonitoringClient(config)

    implicit val sqsClient: AmazonSQSAsync =
      SQSBuilder.buildSQSAsyncClient(config)

    implicit val s3ObjectVerifier: S3ObjectVerifier =
      new S3ObjectVerifier()

    implicit val bagReader: BagReader[_] =
      new S3BagReader()

    implicit val s3Resolvable =
      new S3Resolvable()

    val verifier =
      new BagVerifier()

    val operationName =
      OperationNameBuilder.getName(config)

    val ingestUpdater =
      IngestUpdaterBuilder.build(config, operationName)

    val outgoingPublisher =
      OutgoingPublisherBuilder.build(config, operationName)

    new BagVerifierWorker(
      config = AlpakkaSqsWorkerConfigBuilder.build(config),
      ingestUpdater = ingestUpdater,
      outgoingPublisher = outgoingPublisher,
      verifier = verifier
    )
  }
}
