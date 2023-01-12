package weco.storage_service.bag_verifier

import akka.actor.ActorSystem
import com.typesafe.config.Config
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.messaging.typesafe.SQSBuilder
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage_service.bag_verifier.builder.BagVerifierWorkerBuilder
import weco.storage_service.config.builders.{
  IngestUpdaterBuilder,
  OperationNameBuilder,
  OutgoingPublisherBuilder
}
import weco.typesafe.WellcomeTypesafeApp
import weco.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()

    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()

    implicit val s3Client: S3Client =
      S3Client.builder().build()

    implicit val metrics: CloudWatchMetrics =
      CloudWatchBuilder.buildCloudWatchMetrics(config)

    implicit val sqsClient: SqsAsyncClient =
      SQSBuilder.buildSQSAsyncClient

    val operationName =
      OperationNameBuilder.getName(config)

    val ingestUpdater =
      IngestUpdaterBuilder.build(config, operationName)

    val outgoingPublisher =
      OutgoingPublisherBuilder.build(config, operationName)

    BagVerifierWorkerBuilder.buildBagVerifierWorker(config)(
      ingestUpdater = ingestUpdater,
      outgoingPublisher = outgoingPublisher
    )
  }
}
