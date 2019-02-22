package uk.ac.wellcome.platform.archive.notifier

import akka.actor.ActorSystem
import com.typesafe.config.Config
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.{NotificationStreamBuilder, SNSBuilder}
import uk.ac.wellcome.platform.archive.common.config.builders.HTTPServerBuilder
import uk.ac.wellcome.platform.archive.common.models.CallbackNotification
import uk.ac.wellcome.platform.archive.notifier.services.{CallbackUrlService, NotifierWorkerService}
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem = AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()

    val callbackUrlService = new CallbackUrlService(
      contextURL = HTTPServerBuilder.buildContextURL(config)
    )

    new NotifierWorkerService(
      notificationStream = NotificationStreamBuilder.buildStream[CallbackNotification](config),
      callbackUrlService = callbackUrlService,
      snsWriter = SNSBuilder.buildSNSWriter(config)
    )
  }
}
