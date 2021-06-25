package uk.ac.wellcome.platform.storage.bag_tagger

import akka.actor.ActorSystem
import com.amazonaws.services.s3.AmazonS3
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.json.JsonUtil._
import weco.messaging.typesafe.{
  AlpakkaSqsWorkerConfigBuilder,
  SQSBuilder
}
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage_service.bag_tracker.client.AkkaBagTrackerClient
import uk.ac.wellcome.platform.storage.bag_tagger.services.{
  ApplyTags,
  BagTaggerWorker,
  TagRules
}
import weco.storage.typesafe.S3Builder
import weco.typesafe.WellcomeTypesafeApp
import weco.typesafe.config.builders.AkkaBuilder
import weco.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContextExecutor

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem = AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContextExecutor =
      actorSystem.dispatcher

    implicit val metrics: CloudWatchMetrics =
      CloudWatchBuilder.buildCloudWatchMetrics(config)

    implicit val sqsClient: SqsAsyncClient =
      SQSBuilder.buildSQSAsyncClient(config)

    val bagTrackerClient = new AkkaBagTrackerClient(
      trackerHost = config.requireString("bags.tracker.host")
    )

    implicit val s3Client: AmazonS3 =
      S3Builder.buildS3Client(config)

    new BagTaggerWorker(
      config = AlpakkaSqsWorkerConfigBuilder.build(config),
      metricsNamespace = config.requireString("aws.metrics.namespace"),
      bagTrackerClient = bagTrackerClient,
      applyTags = ApplyTags(),
      tagRules = TagRules.chooseTags
    )
  }
}
