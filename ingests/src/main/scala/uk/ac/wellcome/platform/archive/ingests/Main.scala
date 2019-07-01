package uk.ac.wellcome.platform.archive.ingests

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.{
  AlpakkaSqsWorkerConfigBuilder,
  CloudwatchMonitoringClientBuilder,
  SNSBuilder,
  SQSBuilder
}
import uk.ac.wellcome.messaging.worker.monitoring.CloudwatchMonitoringClient
import uk.ac.wellcome.platform.archive.common.ingests.tracker.dynamo.DynamoIngestTracker
import uk.ac.wellcome.platform.archive.ingests.services.{
  CallbackNotificationService,
  IngestsWorker
}
import uk.ac.wellcome.storage.typesafe.DynamoBuilder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()
    implicit val materializer: ActorMaterializer =
      AkkaBuilder.buildActorMaterializer()

    implicit val monitoringClient: CloudwatchMonitoringClient =
      CloudwatchMonitoringClientBuilder.buildCloudwatchMonitoringClient(config)

    implicit val sqsClient: AmazonSQSAsync =
      SQSBuilder.buildSQSAsyncClient(config)

    implicit val dynamoClient: AmazonDynamoDB =
      DynamoBuilder.buildDynamoClient(config)

    val ingestTracker = new DynamoIngestTracker(
      config = DynamoBuilder.buildDynamoConfig(config),
      bagIdLookupConfig =
        DynamoBuilder.buildDynamoConfig(config, namespace = "bagIdLookup")
    )

    val callbackNotificationService = new CallbackNotificationService(
      messageSender = SNSBuilder.buildSNSMessageSender(
        config,
        subject = "Sent from the ingests service"
      )
    )

    new IngestsWorker(
      alpakkaSQSWorkerConfig = AlpakkaSqsWorkerConfigBuilder.build(config),
      ingestTracker = ingestTracker,
      callbackNotificationService = callbackNotificationService
    )
  }
}
