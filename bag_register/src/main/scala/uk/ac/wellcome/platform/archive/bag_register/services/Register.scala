package uk.ac.wellcome.platform.archive.bag_register.services

import java.time.Instant

import uk.ac.wellcome.platform.archive.bag_register.models.RegistrationSummary
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestCompleted,
  IngestFailed,
  IngestStepResult,
  StorageSpace
}
import uk.ac.wellcome.platform.archive.common.storage.services.{
  StorageManifestService,
  StorageManifestVHS
}
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success, Try}

class Register(
  storageManifestService: StorageManifestService,
  storageManifestVHS: StorageManifestVHS
) {

  def update(
    bagRootLocation: ObjectLocation,
    storageSpace: StorageSpace
  ): Try[IngestStepResult[RegistrationSummary]] = {
    val registration = RegistrationSummary(
      startTime = Instant.now(),
      bagRootLocation = bagRootLocation,
      storageSpace = storageSpace
    )

    for {
      manifest <- storageManifestService
        .retrieve(
          bagRootLocation = bagRootLocation,
          storageSpace = storageSpace
        )

      registrationWithBagId = registration.copy(bagId = Some(manifest.id))

      completedRegistration <- storageManifestVHS
        .updateRecord(manifest)(_ => manifest) match {
        case Success(_) =>
          Success(IngestCompleted(registrationWithBagId.complete))
        case Failure(e) =>
          Success(IngestFailed(registrationWithBagId.complete, e))
      }

    } yield completedRegistration
  }

}
