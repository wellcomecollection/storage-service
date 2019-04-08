package uk.ac.wellcome.platform.archive.bag_register.services

import java.time.Instant

import uk.ac.wellcome.platform.archive.bag_register.models.RegistrationSummary
import uk.ac.wellcome.platform.archive.common.bagit.models.BagLocation
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestCompleted,
  IngestFailed,
  IngestStepResult
}
import uk.ac.wellcome.platform.archive.common.storage.services.{
  StorageManifestService,
  StorageManifestVHS
}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class Register(
  storageManifestService: StorageManifestService,
  storageManifestVHS: StorageManifestVHS
) {

  type FutureSummary =
    Future[IngestStepResult[RegistrationSummary]]

  def update(
    location: BagLocation
  )(implicit
    ec: ExecutionContext): FutureSummary = {

    val registration = RegistrationSummary(
      startTime = Instant.now(),
      location = location
    )

    for {
      manifest <- storageManifestService
        .createManifest(location)

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
