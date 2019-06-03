package uk.ac.wellcome.platform.archive.common.versioning

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID

import scala.util.Try

trait IngestVersionManagerDao {
  def lookupExistingVersion(ingestId: IngestID): Try[Option[VersionRecord]]

  def lookupLatestVersionFor(
    externalIdentifier: ExternalIdentifier): Try[Option[VersionRecord]]

  def storeNewVersion(record: VersionRecord): Try[Unit]
}
