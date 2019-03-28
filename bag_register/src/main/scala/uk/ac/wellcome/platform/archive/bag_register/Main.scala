package uk.ac.wellcome.platform.archive.bag_register

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.Config
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.NotificationStreamBuilder
import uk.ac.wellcome.platform.archive.bag_register.services.{BagRegisterWorker, Register}
import uk.ac.wellcome.platform.archive.common.config.builders.{DiagnosticReporterBuilder, IngestUpdaterBuilder, OutgoingPublisherBuilder}
import uk.ac.wellcome.platform.archive.common.ingests.models.BagRequest
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.{StorageManifestService, StorageManifestVHS}
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
    implicit val materializer: ActorMaterializer =
      AkkaBuilder.buildActorMaterializer()

    implicit val s3Client = S3Builder.buildS3Client(config)

    val storageManifestService = new StorageManifestService()

    val storageManifestVHS = new StorageManifestVHS(
      underlying = VHSBuilder.buildVHS[StorageManifest, EmptyMetadata](config)
    )

    val operationName = "register"

    val ingestUpdater = IngestUpdaterBuilder.build(
      config,
      operationName
    )

    val reporter = DiagnosticReporterBuilder.build(config)

    val register = new Register(
      storageManifestService,
      storageManifestVHS
    )

    val outgoing = OutgoingPublisherBuilder.build(
      config,
      operationName
    )

    val stream = NotificationStreamBuilder
      .buildStream[BagRequest](config)

    new BagRegisterWorker(
      stream,
      ingestUpdater,
      outgoing,
      reporter,
      register
    )
  }
}
