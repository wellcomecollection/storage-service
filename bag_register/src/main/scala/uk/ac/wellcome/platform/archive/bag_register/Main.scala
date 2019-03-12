package uk.ac.wellcome.platform.archive.bag_register

import akka.actor.ActorSystem
import com.typesafe.config.Config
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.NotificationStreamBuilder
import uk.ac.wellcome.platform.archive.bag_register.services.{
  BagRegisterWorker,
  Register
}
import uk.ac.wellcome.platform.archive.common.config.builders.OperationBuilder
import uk.ac.wellcome.platform.archive.common.models.{
  BagRequest,
  StorageManifest
}
import uk.ac.wellcome.platform.archive.common.services.StorageManifestService
import uk.ac.wellcome.platform.archive.common.storage.StorageManifestVHS
import uk.ac.wellcome.storage.typesafe.{S3Builder, VHSBuilder}
import uk.ac.wellcome.storage.vhs.EmptyMetadata
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()

    implicit val ec: ExecutionContext =
      AkkaBuilder.buildExecutionContext()

    implicit val s3Client = S3Builder.buildS3Client(config)

    val storageManifestService = new StorageManifestService()

    val storageManifestVHS = new StorageManifestVHS(
      underlying = VHSBuilder.buildVHS[StorageManifest, EmptyMetadata](config)
    )

    val operationName = "register"

    val notifier = OperationBuilder
      .buildOperationNotifier(config, operationName)

    val reporter = OperationBuilder
      .buildOperationReporter(config)

    val register = new Register(
      storageManifestService,
      storageManifestVHS
    )

    val stream = NotificationStreamBuilder
      .buildStream[BagRequest](config)

    new BagRegisterWorker(
      stream,
      notifier,
      reporter,
      register
    )
  }
}
