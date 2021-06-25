package uk.ac.wellcome.platform.archive.notifier

import akka.actor.ActorSystem
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.http.typesafe.HTTPServerBuilder
import weco.messaging.typesafe.{
  AlpakkaSqsWorkerConfigBuilder,
  SNSBuilder,
  SQSBuilder
}
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import uk.ac.wellcome.platform.archive.notifier.services.{
  CallbackUrlService,
  NotifierWorker
}
import weco.typesafe.WellcomeTypesafeApp
import weco.typesafe.config.builders.AkkaBuilder
import weco.typesafe.config.builders.EnrichConfig._
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
      SQSBuilder.buildSQSAsyncClient(config)

    val callbackUrlService = new CallbackUrlService(
      contextUrl = HTTPServerBuilder.buildContextURL(config),
      client = new AkkaHttpClient()
    )

    new NotifierWorker(
      alpakkaSQSWorkerConfig = AlpakkaSqsWorkerConfigBuilder.build(config),
      callbackUrlService = callbackUrlService,
      messageSender = SNSBuilder.buildSNSMessageSender(
        config,
        subject = "Sent from the notifier"
      ),
      metricsNamespace = config.requireString("aws.metrics.namespace")
    )
  }
}
