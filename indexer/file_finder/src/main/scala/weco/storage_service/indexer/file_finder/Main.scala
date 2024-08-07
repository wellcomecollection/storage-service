package weco.storage_service.indexer.file_finder

import org.apache.pekko.actor.ActorSystem
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.json.JsonUtil._
import weco.messaging.sns.SNSMessageSender
import weco.messaging.typesafe.{PekkoSQSWorkerConfigBuilder, SNSBuilder}
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage_service.bag_tracker.client.PekkoBagTrackerClient
import weco.typesafe.WellcomeTypesafeApp
import weco.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContextExecutor

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      ActorSystem("main-actor-system")

    implicit val executionContext: ExecutionContextExecutor =
      actorSystem.dispatcher

    implicit val metrics: CloudWatchMetrics =
      CloudWatchBuilder.buildCloudWatchMetrics(config)

    implicit val sqsClient: SqsAsyncClient =
      SqsAsyncClient.builder().build()

    implicit val sender: SNSMessageSender =
      SNSBuilder.buildSNSMessageSender(
        config,
        subject = "Sent from the file finder"
      )

    val bagTrackerClient = new PekkoBagTrackerClient(
      trackerHost = config.requireString("bags.tracker.host")
    )

    new FileFinderWorker(
      config = PekkoSQSWorkerConfigBuilder.build(config),
      bagTrackerClient = bagTrackerClient,
      messageSender = sender
    )
  }
}
