package uk.ac.wellcome.platform.archive.common.versioning

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier

import scala.util.{Success, Try}

trait VersionManager {
  protected def lookupExistingVersion(ingestID: IngestID): Try[Option[VersionRecord]]

  protected def lookupLatestVersionFor(externalIdentifier: ExternalIdentifier): Try[Option[VersionRecord]]

  protected def storeNewVersion(record: VersionRecord): Try[Unit]

  private def createNewVersionFor(
    externalIdentifier: ExternalIdentifier,
    ingestId: IngestID,
    ingestDate: Instant
  ): Try[Int] =
    lookupLatestVersionFor(externalIdentifier).flatMap { maybeRecord =>
      Try {
        val newVersion: Int = maybeRecord match {
          case Some(existingRecord) =>
            if (existingRecord.ingestDate.isBefore(ingestDate))
              existingRecord.version + 1
            else
              throw new RuntimeException(s"Latest version has a newer ingest date: ${existingRecord.ingestDate} (stored) > $ingestDate (request)")
          case None => 1
        }

        val newRecord = VersionRecord(
          externalIdentifier = externalIdentifier,
          ingestId = ingestId,
          ingestDate = ingestDate,
          version = newVersion
        )

        storeNewVersion(newRecord)

        newVersion
      }
    }

  def assignVersion(
    externalIdentifier: ExternalIdentifier,
    ingestId: IngestID,
    ingestDate: Instant
  ): Try[Int] =
    lookupExistingVersion(ingestId).flatMap {
      case Some(existingRecord) =>
        if (existingRecord.externalIdentifier == externalIdentifier)
          Success(existingRecord.version)
        else
          throw new RuntimeException(s"External identifiers don't match: ${existingRecord.externalIdentifier} (stored) != $externalIdentifier (request)")

      case None => createNewVersionFor(
        externalIdentifier = externalIdentifier,
        ingestId = ingestId,
        ingestDate = ingestDate
      )
    }
}
