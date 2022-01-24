package weco.storage_service.bag_root_finder

import akka.actor.ActorSystem
import com.amazonaws.services.s3.AmazonS3
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.json.JsonUtil._
import weco.messaging.typesafe.{AlpakkaSqsWorkerConfigBuilder, SQSBuilder}
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage_service.config.builders.{
  IngestUpdaterBuilder,
  OperationNameBuilder,
  OutgoingPublisherBuilder
}
import weco.storage_service.bag_root_finder.services.{
  BagRootFinder,
  BagRootFinderWorker
}
import weco.storage.typesafe.S3Builder
import weco.typesafe.WellcomeTypesafeApp
import weco.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContextExecutor

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem = AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContextExecutor =
      actorSystem.dispatcher

    implicit val s3Client: AmazonS3 = S3Builder.buildS3Client

    implicit val metrics: CloudWatchMetrics =
      CloudWatchBuilder.buildCloudWatchMetrics(config)

    implicit val sqsClient: SqsAsyncClient =
      SQSBuilder.buildSQSAsyncClient

    val operationName = OperationNameBuilder.getName(config)

    new BagRootFinderWorker(
      config = AlpakkaSqsWorkerConfigBuilder.build(config),
      bagRootFinder = new BagRootFinder(),
      ingestUpdater = IngestUpdaterBuilder.build(config, operationName),
      outgoingPublisher = OutgoingPublisherBuilder.build(config, operationName),
    )
  }
}
