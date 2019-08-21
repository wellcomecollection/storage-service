package uk.ac.wellcome.platform.archive.bag_register.services

import java.time.Instant

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bag_register.models.RegistrationSummary
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.archive.common.storage.services.{
  BadFetchLocationException,
  StorageManifestDao,
  StorageManifestService
}

import scala.util.{Failure, Success, Try}

class Register(
  bagReader: BagReader[_],
  storageManifestDao: StorageManifestDao,
  storageManifestService: StorageManifestService
) extends Logging {

  def update(
    location: PrimaryStorageLocation,
    replicas: Seq[SecondaryStorageLocation],
    version: BagVersion,
    space: StorageSpace
  ): Try[IngestStepResult[RegistrationSummary]] = {
    val registration = RegistrationSummary(
      startTime = Instant.now(),
      location = location,
      space = space
    )

    val result: Try[IngestStepResult[RegistrationSummary]] = for {
      bag <- bagReader.get(location.prefix) match {
        case Right(value) => Success(value)
        case Left(err) =>
          Failure(new RuntimeException(s"Bag unavailable: ${err.msg}"))
      }

      storageManifest <- storageManifestService.createManifest(
        bag = bag,
        location = location,
        replicas = replicas,
        space = space,
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

      case Failure(err: BadFetchLocationException) =>
        Success(
          IngestFailed(
            registration.complete,
            err,
            maybeUserFacingMessage = Some(err.getMessage)
          )
        )

      case Failure(err) => Success(IngestFailed(registration.complete, err))
    }
  }
}
