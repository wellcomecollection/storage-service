package uk.ac.wellcome.platform.archive.bag_register.services

import java.time.Instant

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bag_register.models.RegistrationSummary
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.archive.common.storage.services.{
  StorageManifestService,
  StorageManifestVHS
}
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Success, Try}

class Register(
  storageManifestService: StorageManifestService,
  storageManifestVHS: StorageManifestVHS
) extends Logging {

  def update(
    bagRootLocation: ObjectLocation,
    storageSpace: StorageSpace
  ): Try[IngestStepResult[RegistrationSummary]] = {
    val registration = RegistrationSummary(
      startTime = Instant.now(),
      bagRootLocation = bagRootLocation,
      storageSpace = storageSpace
    )

    val result = for {
      manifest <- storageManifestService
        .retrieve(
          bagRootLocation = bagRootLocation,
          storageSpace = storageSpace
        )

      registrationWithBagId = registration.copy(bagId = Some(manifest.id))

      completedRegistration <- storageManifestVHS
        .updateRecord(manifest)(_ => manifest) match {
        case Right(_) =>
          Right(IngestCompleted(registrationWithBagId.complete))
        case Left(storageError) =>
          error("Unexpected error updating storage manifest", storageError.e)
          Right(IngestFailed(registrationWithBagId.complete, storageError.e))
      }
    } yield completedRegistration

    result match {
      case Right(stepResult) => Success(stepResult)
      case Left(value)       => Success(IngestFailed(registration.complete, value))
    }
  }
}
