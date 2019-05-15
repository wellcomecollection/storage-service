package uk.ac.wellcome.platform.storage.bagauditor.versioning

import java.time.Instant
import java.util.UUID

import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.versioning.IngestVersionManager
import uk.ac.wellcome.storage.{LockDao, LockingService}

import scala.util.{Success, Try}

class VersionPicker(
  lockingService: LockingService[Int, Try, LockDao[String, UUID]],
  ingestVersionManager: IngestVersionManager
) {
  def chooseVersion(
    externalIdentifier: ExternalIdentifier,
    ingestId: IngestID,
    ingestDate: Instant
  ): Try[Int] = Success(1)
}
