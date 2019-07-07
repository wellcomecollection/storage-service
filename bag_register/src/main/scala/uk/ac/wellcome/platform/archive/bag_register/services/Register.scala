package uk.ac.wellcome.platform.archive.bag_register.services

import java.time.Instant

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bag_register.models.RegistrationSummary
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  InfrequentAccessStorageProvider,
  StorageLocation
}
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Success, Try}

class Register(
  bagReader: BagReader[_],
  storageManifestDao: StorageManifestDao
) extends Logging {

  def update(
    bagRootLocation: ObjectLocation,
    version: Int,
    storageSpace: StorageSpace
  ): Try[IngestStepResult[RegistrationSummary]] = {

    val registration = RegistrationSummary(
      startTime = Instant.now(),
      bagRootLocation = bagRootLocation,
      storageSpace = storageSpace
    )

    val result = for {
      bag <- bagReader.get(bagRootLocation)

      manifest = StorageManifest(
        space = storageSpace,
        info = bag.info,
        version = version,
        manifest = bag.manifest,
        tagManifest = bag.tagManifest,
        locations = List(
          StorageLocation(
            provider = InfrequentAccessStorageProvider,
            location = bagRootLocation
          )
        ),
        createdDate = Instant.now()
      )

      registrationWithBagId = registration.copy(bagId = Some(manifest.id))

      completedRegistration <- storageManifestDao.put(manifest) match {
        case Right(_) =>
          Right(IngestCompleted(registrationWithBagId.complete))
        case Left(storageError) =>
          error("Unexpected error updating storage manifest", storageError.e)
          Right(IngestFailed(registrationWithBagId.complete, storageError.e))
      }
    } yield completedRegistration

    result match {
      case Right(stepResult) => Success(stepResult)
      case Left(value) => Success(IngestFailed(registration.complete, value))
    }
  }
}
