package uk.ac.wellcome.platform.storage.bagauditor.versioning

import java.time.Instant
import java.util.UUID

import cats.implicits._
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.{CreateIngestType, IngestID, IngestType, UpdateIngestType}
import uk.ac.wellcome.platform.archive.common.versioning.IngestVersionManager
import uk.ac.wellcome.platform.storage.bagauditor.models.{IngestTypeCreateForExistingBag, IngestTypeUpdateForNewBag, InternalVersionPickerError, VersionPickerError}
import uk.ac.wellcome.storage.{FailedProcess, LockDao, LockingService}

import scala.util.{Failure, Success, Try}

class VersionPicker(
  lockingService: LockingService[Int, Try, LockDao[String, UUID]],
  ingestVersionManager: IngestVersionManager
) {
  def chooseVersion(
    externalIdentifier: ExternalIdentifier,
    ingestId: IngestID,
    ingestType: IngestType = CreateIngestType,
    ingestDate: Instant
  ): Either[VersionPickerError, Int] = {
    val tryVersion: Try[lockingService.Process] = lockingService
      .withLocks(Set(s"ingest:$ingestId", s"external:$externalIdentifier")) {
        ingestVersionManager.assignVersion(
          externalIdentifier = externalIdentifier,
          ingestId = ingestId,
          ingestDate = ingestDate
        )
      }

    tryVersion match {
      case Success(Right(version))              => checkVersionIsAllowed(ingestType, assignedVersion = version)
      case Success(Left(FailedProcess(_, err))) => Left(InternalVersionPickerError(err))
      case Success(Left(err))                   => Left(InternalVersionPickerError(new Throwable(s"Locking error: $err")))
      case Failure(err)                         => Left(InternalVersionPickerError(err))
    }
  }

  private def checkVersionIsAllowed(ingestType: IngestType,
                                    assignedVersion: Int): Either[VersionPickerError, Int] =
    if (ingestType == CreateIngestType && assignedVersion > 1) {
      Left(IngestTypeCreateForExistingBag())
    } else if (ingestType == UpdateIngestType && assignedVersion == 1) {
      Left(IngestTypeUpdateForNewBag())
    } else {
      Right(assignedVersion)
    }
}
