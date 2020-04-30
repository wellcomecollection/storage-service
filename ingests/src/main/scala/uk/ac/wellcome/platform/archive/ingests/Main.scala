package uk.ac.wellcome.platform.archive.ingests

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.typesafe.{
  AlpakkaSqsWorkerConfigBuilder,
  CloudwatchMonitoringClientBuilder,
  SNSBuilder,
  SQSBuilder
}
import uk.ac.wellcome.messaging.worker.monitoring.metrics.cloudwatch.CloudwatchMetricsMonitoringClient
import uk.ac.wellcome.platform.archive.common.ingests.tracker.dynamo.DynamoIngestTracker
import uk.ac.wellcome.platform.archive.ingests.services.{
  CallbackNotificationService,
  IngestsWorker
}
import uk.ac.wellcome.storage.typesafe.DynamoBuilder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()
    implicit val materializer: Materializer =
      AkkaBuilder.buildMaterializer()

    implicit val monitoringClient: CloudwatchMetricsMonitoringClient =
      CloudwatchMonitoringClientBuilder.buildCloudwatchMonitoringClient(config)

    implicit val sqsClient: SqsAsyncClient =
      SQSBuilder.buildSQSAsyncClient(config)

    implicit val dynamoClient: AmazonDynamoDB =
      DynamoBuilder.buildDynamoClient(config)

    val ingestTracker = new DynamoIngestTracker(
      config = DynamoBuilder.buildDynamoConfig(config)
    )

    val callbackNotificationService = new CallbackNotificationService(
      messageSender = SNSBuilder.buildSNSMessageSender(
        config,
        namespace = "callbackNotifications",
        subject = "Sent from the ingests service"
      )
    )

    val updatedIngestsMessageSender = SNSBuilder.buildSNSMessageSender(
      config,
      namespace = "updatedIngests",
      subject = "Updated ingests sent by the ingests monitor"
    )

    new IngestsWorker(
      alpakkaSQSWorkerConfig = AlpakkaSqsWorkerConfigBuilder.build(config),
      ingestTracker = ingestTracker,
      callbackNotificationService = callbackNotificationService,
      updatedIngestsMessageSender = updatedIngestsMessageSender,
      metricsNamespace = config.required[String]("aws.metrics.namespace")
    )
  }
}
