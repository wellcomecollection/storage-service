package weco.storage_service.notifier

import org.apache.pekko.actor.ActorSystem
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.http.client.PekkoHttpClient
import weco.messaging.typesafe.{PekkoSQSWorkerConfigBuilder, SNSBuilder}
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage_service.notifier.services.{
  CallbackUrlService,
  NotifierWorker
}
import weco.typesafe.WellcomeTypesafeApp

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

    val callbackUrlService = new CallbackUrlService(
      client = new PekkoHttpClient()
    )

    new NotifierWorker(
      config = PekkoSQSWorkerConfigBuilder.build(config),
      callbackUrlService = callbackUrlService,
      messageSender = SNSBuilder.buildSNSMessageSender(
        config,
        subject = "Sent from the notifier"
      )
    )
  }
}
