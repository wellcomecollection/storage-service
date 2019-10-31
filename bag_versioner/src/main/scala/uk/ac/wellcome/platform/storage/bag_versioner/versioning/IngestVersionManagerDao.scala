package uk.ac.wellcome.platform.storage.bag_versioner.versioning

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.MaximaError

import scala.util.Try

trait IngestVersionManagerDao {
  def lookupExistingVersion(ingestId: IngestID): Try[Option[VersionRecord]]

  def lookupLatestVersionFor(
    externalIdentifier: ExternalIdentifier,
    storageSpace: StorageSpace
  ): Either[MaximaError, VersionRecord]

  def storeNewVersion(record: VersionRecord): Try[Unit]
}
