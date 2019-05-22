package uk.ac.wellcome.platform.archive.bag_register.services

import java.time.Instant

import uk.ac.wellcome.platform.archive.bag_register.models.RegistrationSummary
import uk.ac.wellcome.platform.archive.common.bagit.services.BagUnavailable
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.archive.common.storage.services.{StorageManifestService, StorageManifestVHS}
import uk.ac.wellcome.storage.ObjectLocation

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class Register(
  storageManifestService: StorageManifestService,
  storageManifestVHS: StorageManifestVHS
)(implicit ec: ExecutionContext) {

  type FutureSummary =
    Future[IngestStepResult[RegistrationSummary]]

  def update(
    bagRootLocation: ObjectLocation,
    storageSpace: StorageSpace
  ): FutureSummary = {
    val registration = RegistrationSummary(
      startTime = Instant.now(),
      bagRootLocation = bagRootLocation,
      storageSpace = storageSpace
    )

    for {
      manifest <- Future.fromTry {
        val result: Either[BagUnavailable, StorageManifest] = storageManifestService
          .retrieve(
            bagRootLocation = bagRootLocation,
            storageSpace = storageSpace
          )

        result match {
          case Right(storageManifest) => Success(storageManifest)
          case Left(bagUnavailable) => Failure(bagUnavailable)
        }
      }

      registrationWithBagId = registration.copy(bagId = Some(manifest.id))

      completedRegistration <- storageManifestVHS
        .updateRecord(manifest)(_ => manifest)
        .transform {
          case Success(_) =>
            Success(IngestCompleted(registrationWithBagId.complete))
          case Failure(e) =>
            Success(IngestFailed(registrationWithBagId.complete, e))
        }

    } yield completedRegistration
  }
}
