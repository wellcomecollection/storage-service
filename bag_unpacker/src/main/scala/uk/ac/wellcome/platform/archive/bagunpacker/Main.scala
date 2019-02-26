package uk.ac.wellcome.platform.archive.bagunpacker

import akka.actor.ActorSystem
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.{NotificationStreamBuilder, SNSBuilder}
import uk.ac.wellcome.platform.archive.bagunpacker.services.BagUnpackerWorkerService
import uk.ac.wellcome.platform.archive.common.models.UnpackRequest
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

import uk.ac.wellcome.json.JsonUtil._

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem = AkkaBuilder.buildActorSystem()

    implicit val ec = actorSystem.dispatcher

    new BagUnpackerWorkerService(
      stream = NotificationStreamBuilder.buildStream[UnpackRequest](config),
      progressSnsWriter =
        SNSBuilder.buildSNSWriter(config, namespace = "progress"),
      outgoingSnsWriter =
        SNSBuilder.buildSNSWriter(config, namespace = "outgoing")
    )
  }
}
