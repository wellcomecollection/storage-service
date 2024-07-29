package weco.storage_service.ingests_tracker

import org.apache.pekko.actor.ActorSystem
import com.typesafe.config.Config
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import weco.messaging.sns.{SNSConfig, SNSMessageSender}
import weco.messaging.typesafe.SNSBuilder
import weco.storage_service.ingests_tracker.services.{
  CallbackNotificationService,
  MessagingService
}
import weco.storage_service.ingests_tracker.tracker.IngestTracker
import weco.storage_service.ingests_tracker.tracker.dynamo.DynamoIngestTracker
import weco.storage.typesafe.DynamoBuilder
import weco.typesafe.WellcomeTypesafeApp

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      ActorSystem("main-actor-system")

    implicit val dynamoClient: DynamoDbClient =
      DynamoDbClient.builder().build()

    val callbackNotificationService: CallbackNotificationService[SNSConfig] =
      new CallbackNotificationService(
        messageSender = SNSBuilder.buildSNSMessageSender(
          config,
          namespace = "callbackNotifications",
          subject = "Sent from the ingests service"
        )
      )

    val updatedIngestsMessageSender: SNSMessageSender =
      SNSBuilder.buildSNSMessageSender(
        config,
        namespace = "updatedIngests",
        subject = "Updated ingests sent by the ingests monitor"
      )

    val messagingService = new MessagingService(
      callbackNotificationService,
      updatedIngestsMessageSender
    )

    val ingestTracker: IngestTracker = new DynamoIngestTracker(
      config = DynamoBuilder.buildDynamoConfig(config)
    )

    new IngestsTrackerApi[SNSConfig, SNSConfig](
      ingestTracker,
      messagingService
    )()
  }
}
