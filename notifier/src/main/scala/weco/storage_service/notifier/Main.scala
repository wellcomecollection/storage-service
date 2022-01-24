package weco.storage_service.notifier

import akka.actor.ActorSystem
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.messaging.typesafe.{
  AlpakkaSqsWorkerConfigBuilder,
  SNSBuilder,
  SQSBuilder
}
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage_service.notifier.services.{
  CallbackUrlService,
  NotifierWorker
}
import weco.typesafe.WellcomeTypesafeApp
import weco.typesafe.config.builders.AkkaBuilder
import weco.http.client.AkkaHttpClient

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()

    implicit val metrics: CloudWatchMetrics =
      CloudWatchBuilder.buildCloudWatchMetrics(config)

    implicit val sqsClient: SqsAsyncClient =
      SQSBuilder.buildSQSAsyncClient

    val callbackUrlService = new CallbackUrlService(
      client = new AkkaHttpClient()
    )

    new NotifierWorker(
      config = AlpakkaSqsWorkerConfigBuilder.build(config),
      callbackUrlService = callbackUrlService,
      messageSender = SNSBuilder.buildSNSMessageSender(
        config,
        subject = "Sent from the notifier"
      )
    )
  }
}
