package uk.ac.wellcome.platform.storage.ingests_tracker

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.sns.{SNSConfig, SNSMessageSender}
import uk.ac.wellcome.messaging.typesafe.SNSBuilder
import uk.ac.wellcome.platform.storage.ingests_tracker.services.{
  CallbackNotificationService,
  MessagingService
}
import uk.ac.wellcome.platform.storage.ingests_tracker.tracker.IngestTracker
import uk.ac.wellcome.platform.storage.ingests_tracker.tracker.dynamo.DynamoIngestTracker
import uk.ac.wellcome.storage.typesafe.DynamoBuilder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()
    implicit val materializer: Materializer =
      AkkaBuilder.buildMaterializer()

    implicit val dynamoClient: AmazonDynamoDB =
      DynamoBuilder.buildDynamoClient(config)

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
