package uk.ac.wellcome.platform.archive.indexer.file_finder

import akka.actor.ActorSystem
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.SNSMessageSender
import uk.ac.wellcome.messaging.typesafe.{AlpakkaSqsWorkerConfigBuilder, CloudwatchMonitoringClientBuilder, SNSBuilder, SQSBuilder}
import uk.ac.wellcome.messaging.worker.monitoring.metrics.cloudwatch.CloudwatchMetricsMonitoringClient
import uk.ac.wellcome.platform.archive.bag_tracker.client.AkkaBagTrackerClient
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContextExecutor

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem = AkkaBuilder.buildActorSystem()

    implicit val executionContext: ExecutionContextExecutor =
      actorSystem.dispatcher

    implicit val monitoringClient: CloudwatchMetricsMonitoringClient =
      CloudwatchMonitoringClientBuilder.buildCloudwatchMonitoringClient(config)

    implicit val sqsClient: SqsAsyncClient =
      SQSBuilder.buildSQSAsyncClient(config)

    implicit val sender: SNSMessageSender =
      SNSBuilder.buildSNSMessageSender(config, subject = "Sent from the file finder")

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

