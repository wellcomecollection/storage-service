package uk.ac.wellcome.platform.storage.bagauditor.versioning

import java.time.Instant
import java.util.UUID

import cats.implicits._
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.{CreateIngestType, IngestID, IngestType, UpdateIngestType}
import uk.ac.wellcome.platform.archive.common.versioning.IngestVersionManager
import uk.ac.wellcome.storage.{FailedProcess, LockDao, LockingService}

import scala.util.Try

class IllegalVersionAssignment(message: String) extends RuntimeException(message)

class VersionPicker(
  lockingService: LockingService[Int, Try, LockDao[String, UUID]],
  ingestVersionManager: IngestVersionManager
) {
  def chooseVersion(
    externalIdentifier: ExternalIdentifier,
    ingestId: IngestID,
    ingestType: IngestType = CreateIngestType,
    ingestDate: Instant
  ): Try[Int] =
    lockingService
      .withLocks(Set(s"ingest:$ingestId", s"external:$externalIdentifier")) {
        ingestVersionManager.assignVersion(
          externalIdentifier = externalIdentifier,
          ingestId = ingestId,
          ingestDate = ingestDate
        )
      }
      .map {
        case Right(version)              =>
          checkVersionIsAllowed(ingestType, assignedVersion = version)
          version

        case Left(FailedProcess(_, err)) => throw err
        case Left(err)                   => throw new RuntimeException(s"Locking error: $err")
      }

  private def checkVersionIsAllowed(ingestType: IngestType, assignedVersion: Int): Unit = {
    if (ingestType == CreateIngestType && assignedVersion > 1) {
      throw new IllegalVersionAssignment("Ingest type 'create' is not allowed for a bag that already exists")
    } else if (ingestType == UpdateIngestType && assignedVersion == 1) {
      throw new IllegalVersionAssignment("Ingest type 'update' is not allowed unless a bag already exists")
    }
  }
}
