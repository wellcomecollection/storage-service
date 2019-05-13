package uk.ac.wellcome.platform.archive.common.versioning

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier

import scala.util.{Failure, Try}

trait VersionManager {
  protected def lookupExistingVersion(ingestID: IngestID): Try[Option[VersionRecord]]

  protected def lookupLatestVersionFor(externalIdentifier: ExternalIdentifier): Try[Option[VersionRecord]]

  protected def storeNewVersion(record: VersionRecord): Try[Unit]

  def assignVersion(
    externalIdentifier: ExternalIdentifier,
    ingestId: IngestID,
    ingestDate: Instant
  ): Try[Int] = Failure(new Throwable("BOOM!"))
}
