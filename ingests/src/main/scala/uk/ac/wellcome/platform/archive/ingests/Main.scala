package uk.ac.wellcome.platform.archive.ingests

import akka.actor.ActorSystem
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.sns.NotificationMessage
import uk.ac.wellcome.messaging.typesafe.{SNSBuilder, SQSBuilder}
import uk.ac.wellcome.platform.archive.common.progress.monitor.ProgressTracker
import uk.ac.wellcome.platform.archive.ingests.services.{
  CallbackNotificationService,
  IngestsWorkerService
}
import uk.ac.wellcome.storage.typesafe.DynamoBuilder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem = AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()

    val progressTracker = new ProgressTracker(
      dynamoDbClient = DynamoBuilder.buildDynamoClient(config),
      dynamoConfig = DynamoBuilder.buildDynamoConfig(config)
    )

    val callbackNotificationService = new CallbackNotificationService(
      snsWriter = SNSBuilder.buildSNSWriter(config)
    )

    new IngestsWorkerService(
      sqsStream = SQSBuilder.buildSQSStream[NotificationMessage](config),
      progressTracker = progressTracker,
      callbackNotificationService = callbackNotificationService
    )
  }
}
