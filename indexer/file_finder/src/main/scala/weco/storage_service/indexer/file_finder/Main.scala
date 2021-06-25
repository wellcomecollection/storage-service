package weco.storage_service.indexer.file_finder

import akka.actor.ActorSystem
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.json.JsonUtil._
import weco.messaging.sns.SNSMessageSender
import weco.messaging.typesafe.{
  AlpakkaSqsWorkerConfigBuilder,
  SNSBuilder,
  SQSBuilder
}
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage_service.bag_tracker.client.AkkaBagTrackerClient
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

    implicit val sender: SNSMessageSender =
      SNSBuilder.buildSNSMessageSender(
        config,
        subject = "Sent from the file finder"
      )

    val bagTrackerClient = new AkkaBagTrackerClient(
      trackerHost = config.requireString("bags.tracker.host")
    )

    new FileFinderWorker(
      config = AlpakkaSqsWorkerConfigBuilder.build(config),
      bagTrackerClient = bagTrackerClient,
      metricsNamespace = config.requireString("aws.metrics.namespace"),
      messageSender = sender
    )
  }
}
