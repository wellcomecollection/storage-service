package uk.ac.wellcome.platform.archive.bag_register

import akka.actor.ActorSystem
import com.typesafe.config.Config
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.{NotificationStreamBuilder, SNSBuilder}
import uk.ac.wellcome.platform.archive.bag_register.services.BagsWorkerService
import uk.ac.wellcome.platform.archive.common.models.{
  ReplicationResult,
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
    implicit val actorSystem: ActorSystem = AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()

    implicit val s3Client = S3Builder.buildS3Client(config)

    val storageManifestService = new StorageManifestService()

    val storageManifestVHS = new StorageManifestVHS(
      underlying = VHSBuilder.buildVHS[StorageManifest, EmptyMetadata](config)
    )

    new BagsWorkerService(
      notificationStream =
        NotificationStreamBuilder.buildStream[ReplicationResult](config),
      storageManifestService = storageManifestService,
      storageManifestVHS = storageManifestVHS,
      progressSnsWriter = SNSBuilder.buildSNSWriter(config)
    )
  }
}
