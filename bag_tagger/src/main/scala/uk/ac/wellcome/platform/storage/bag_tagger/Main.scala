package uk.ac.wellcome.platform.storage.bag_tagger

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.amazonaws.services.s3.AmazonS3
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.{
  AlpakkaSqsWorkerConfigBuilder,
  CloudwatchMonitoringClientBuilder,
  SQSBuilder
}
import uk.ac.wellcome.messaging.worker.monitoring.metrics.cloudwatch.CloudwatchMetricsMonitoringClient
import uk.ac.wellcome.platform.archive.bag_tracker.client.AkkaBagTrackerClient
import uk.ac.wellcome.platform.storage.bag_tagger.services.{
  ApplyTags,
  BagTaggerWorker,
  TagRules
}
import uk.ac.wellcome.storage.tags.s3.NewS3Tags
import uk.ac.wellcome.storage.typesafe.S3Builder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContextExecutor

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem = AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContextExecutor =
      actorSystem.dispatcher
    implicit val mat: Materializer =
      AkkaBuilder.buildMaterializer()

    implicit val monitoringClient: CloudwatchMetricsMonitoringClient =
      CloudwatchMonitoringClientBuilder.buildCloudwatchMonitoringClient(config)

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
      applyTags = new ApplyTags(
        s3Tags = new NewS3Tags()
      ),
      tagRules = TagRules.chooseTags
    )
  }
}
