package weco.storage_service.bag_versioner.versioning

import java.time.Instant

import weco.storage_service.bagit.models.ExternalIdentifier
import weco.storage_service.storage.models.StorageSpace

sealed trait IngestVersionManagerError

case class IngestVersionManagerDaoError(e: Throwable)
    extends IngestVersionManagerError

case class ExternalIdentifiersMismatch(
  stored: ExternalIdentifier,
  request: ExternalIdentifier
) extends IngestVersionManagerError

case class StorageSpaceMismatch(stored: StorageSpace, request: StorageSpace)
    extends IngestVersionManagerError

case class NewerIngestAlreadyExists(stored: Instant, request: Instant)
    extends IngestVersionManagerError

case class IngestTypeCreateForExistingBag(existingRecord: VersionRecord)
    extends IngestVersionManagerError
case class IngestTypeUpdateForNewBag() extends IngestVersionManagerError
