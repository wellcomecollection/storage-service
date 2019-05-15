package uk.ac.wellcome.platform.storage.bagauditor.versioning

import java.time.Instant
import java.util.UUID

import cats.implicits._
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.versioning.IngestVersionManager
import uk.ac.wellcome.storage.{FailedProcess, LockDao, LockingService}

import scala.util.Try

class VersionPicker(
  lockingService: LockingService[Int, Try, LockDao[String, UUID]],
  ingestVersionManager: IngestVersionManager
) {
  def chooseVersion(
    externalIdentifier: ExternalIdentifier,
    ingestId: IngestID,
    ingestDate: Instant
  ): Try[Int] =
    lockingService.withLocks(Set(s"ingest:$ingestId", s"external:$externalIdentifier")) {
      ingestVersionManager.assignVersion(
        externalIdentifier = externalIdentifier,
        ingestId = ingestId,
        ingestDate = ingestDate
      )
    }.map {
      case Right(version) => version
      case Left(FailedProcess(_, err)) => throw err
      case Left(err) => throw new RuntimeException(s"Locking error: $err")
    }
}
