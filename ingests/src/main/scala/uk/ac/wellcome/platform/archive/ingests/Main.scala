package uk.ac.wellcome.platform.archive.ingests

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.Config
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.{NotificationStreamBuilder, SNSBuilder}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestUpdate
import uk.ac.wellcome.platform.archive.common.ingests.monitor.IngestTracker
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
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()
    implicit val materializer: ActorMaterializer =
      AkkaBuilder.buildActorMaterializer()

    val ingestTracker = new IngestTracker(
      dynamoDbClient = DynamoBuilder.buildDynamoClient(config),
      dynamoConfig = DynamoBuilder.buildDynamoConfig(config)
    )

    val callbackNotificationService = new CallbackNotificationService(
      snsWriter = SNSBuilder.buildSNSWriter(config)
    )

    new IngestsWorkerService(
      notificationStream =
        NotificationStreamBuilder.buildStream[IngestUpdate](config),
      ingestTracker = ingestTracker,
      callbackNotificationService = callbackNotificationService
    )
  }
}
