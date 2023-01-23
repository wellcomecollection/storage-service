package weco.storage_service.bag_tagger

import akka.actor.ActorSystem
import com.typesafe.config.Config
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.json.JsonUtil._
import weco.messaging.typesafe.AlpakkaSqsWorkerConfigBuilder
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage_service.bag_tagger.services.{ApplyTags, BagTaggerWorker, TagRules}
import weco.storage_service.bag_tracker.client.AkkaBagTrackerClient
import weco.typesafe.WellcomeTypesafeApp
import weco.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      ActorSystem("main-actor-system")
    implicit val ec: ExecutionContext =
      actorSystem.dispatcher

    implicit val metrics: CloudWatchMetrics =
      CloudWatchBuilder.buildCloudWatchMetrics(config)

    implicit val sqsClient: SqsAsyncClient =
      SqsAsyncClient.builder().build()

    val bagTrackerClient = new AkkaBagTrackerClient(
      trackerHost = config.requireString("bags.tracker.host")
    )

    implicit val s3Client: S3Client =
      S3Client.builder().build()

    new BagTaggerWorker(
      config = AlpakkaSqsWorkerConfigBuilder.build(config),
      bagTrackerClient = bagTrackerClient,
      applyTags = ApplyTags(),
      tagRules = TagRules.chooseTags
    )
  }
}
