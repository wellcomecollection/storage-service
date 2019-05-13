package uk.ac.wellcome.platform.archive.common.versioning

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier

import scala.util.{Success, Try}

trait VersionManager {
  protected def lookupExistingVersion(ingestID: IngestID): Try[Option[VersionRecord]]

  protected def lookupLatestVersionFor(externalIdentifier: ExternalIdentifier): Try[Option[VersionRecord]]

  protected def storeNewVersion(record: VersionRecord): Try[Unit]

  def assignVersion(
    externalIdentifier: ExternalIdentifier,
    ingestId: IngestID,
    ingestDate: Instant
  ): Try[Int] = lookupLatestVersionFor(externalIdentifier).flatMap { maybeRecord =>
    val newVersion: Int = maybeRecord match {
      case Some(existingRecord) => existingRecord.version + 1
      case None                 => 1
    }

    val newRecord = VersionRecord(
      externalIdentifier = externalIdentifier,
      ingestId = ingestId,
      ingestDate = ingestDate,
      version = newVersion
    )

    storeNewVersion(newRecord)

    Success(newVersion)
  }
}
