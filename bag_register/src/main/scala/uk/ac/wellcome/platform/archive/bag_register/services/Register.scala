package uk.ac.wellcome.platform.archive.bag_register.services

import java.time.Instant

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bag_register.models.RegistrationSummary
import uk.ac.wellcome.platform.archive.common.bagit.services.BagDao
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  InfrequentAccessStorageProvider,
  StorageLocation
}
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Success, Try}

class Register(
  bagService: BagDao,
  storageManifestVHS: StorageManifestDao
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
      bag <- bagService.get(bagRootLocation)

      manifest = StorageManifest.create(
        root = bagRootLocation,
        space = storageSpace,
        bag = bag,
        locations = List(
          StorageLocation(
            provider = InfrequentAccessStorageProvider,
            location = bagRootLocation
          )
        )
      )

      registrationWithBagId = registration.copy(bagId = Some(manifest.id))

      completedRegistration <- storageManifestVHS
        .update(manifest)(_ => manifest) match {
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
