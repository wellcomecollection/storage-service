package uk.ac.wellcome.platform.archive.bag_register.services

import java.time.Instant

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bag_register.models.RegistrationSummary
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestCompleted, IngestFailed, IngestStepResult, StorageSpace}
import uk.ac.wellcome.platform.archive.common.storage.services.{StorageManifestDao, StorageManifestService}
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success, Try}

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

    val result: Try[IngestStepResult[RegistrationSummary]] = for {
      bag <- bagReader.get(bagRootLocation) match {
        case Right(value) => Success(value)
        case Left(err)    => Failure(new RuntimeException(s"Bag unavailable: ${err.msg}"))
      }

      storageManifest <- StorageManifestService.createManifest(
        bag = bag,
        replicaRoot = bagRootLocation,
        space = storageSpace,
        version = version
      )

      completedRegistration <- storageManifestDao.put(storageManifest) match {
        case Right(_) =>
          Success(IngestCompleted(registration.complete))
        case Left(storageError) =>
          error("Unexpected error updating storage manifest", storageError.e)
          Success(IngestFailed(registration.complete, storageError.e))
      }
    } yield completedRegistration

    result match {
      case Success(stepResult) => Success(stepResult)
      case Failure(err)        => Success(IngestFailed(registration.complete, err))
    }
  }
}
