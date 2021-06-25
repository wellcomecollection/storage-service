package weco.storage_service.bag_versioner.versioning

import weco.storage_service.bagit.models.ExternalIdentifier
import weco.storage_service.ingests.models.IngestID
import weco.storage_service.storage.models.StorageSpace
import weco.storage.MaximaError

import scala.util.Try

trait IngestVersionManagerDao {
  def lookupExistingVersion(ingestId: IngestID): Try[Option[VersionRecord]]

  def lookupLatestVersionFor(
    externalIdentifier: ExternalIdentifier,
    storageSpace: StorageSpace
  ): Either[MaximaError, VersionRecord]

  def storeNewVersion(record: VersionRecord): Try[Unit]
}
